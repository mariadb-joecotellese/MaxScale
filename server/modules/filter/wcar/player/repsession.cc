#include "repsession.hh"
#include "repplayer.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <maxbase/string.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <thread>

namespace
{
namespace ThisUnit
{
std::atomic<int> next_thread_idx = 0;
thread_local int thread_idx = -1;
}
}

bool RepSession::execute_stmt(const QueryEvent& qevent)
{
    bool count_rows = m_config.row_counts;
    auto sql = maxsimd::canonical_args_to_sql(*qevent.sCanonical, qevent.canonical_args);

    if (count_rows)
    {
        sql += ";SHOW STATUS WHERE Variable_name IN ('Rows_read')";
    }

    RepEvent revent;
    revent.can_id = qevent.can_id;
    revent.event_id = qevent.event_id;
    revent.start_time = mxb::Clock::now();
    revent.num_rows = 0;
    revent.rows_read = 0;
    revent.error = 0;

    if (mysql_query(m_pConn, sql.c_str()))
    {
        int error_number = mysql_errno(m_pConn);
        MXB_SERROR("MariaDB: Error S "
                   << qevent.session_id << " E " << qevent.event_id
                   << " SQL " << mxb::show_some(sql)
                   << " Error code " << error_number << ": " << mysql_error(m_pConn));
        revent.error = error_number;
    }

    bool more_results;

    do
    {
        more_results = mysql_more_results(m_pConn);

        if (MYSQL_RES* result = mysql_store_result(m_pConn))
        {
            if (count_rows && !more_results)
            {
                mxb_assert(mysql_num_fields(result) == 2);

                // This is the result of the SHOW STATUS command, store the counters from it.
                while (MYSQL_ROW row = mysql_fetch_row(result))
                {
                    // NOTE: If more values are ever added to the IN list, the following code must be modified
                    // to compare the row values to make sure the right one is processed. Right now the query
                    // always returns only one row.
                    int64_t rows_read = atol(row[1]);
                    revent.rows_read = rows_read - m_rows_read;
                    m_rows_read = rows_read;
                }
            }
            else
            {
                revent.num_rows += mysql_num_rows(result);
            }

            mysql_free_result(result);
        }

        mysql_next_result(m_pConn);
    }
    while (more_results);

    revent.end_time = mxb::Clock::now();

    if (is_real_event(qevent))
    {
        m_pRecorder->get_shared_data_by_index(ThisUnit::thread_idx)->send_update(std::move(revent));
    }

    return true;
}

RepSession::RepSession(const RepConfig* pConfig,
                       RepPlayer* pPlayer,
                       int64_t session_id,
                       RepRecorder* pRecorder,
                       maxbase::ThreadPool& tpool)
    : m_config(*pConfig)
    , m_player(*pPlayer)
    , m_session_id(session_id)
    , m_pRecorder(pRecorder)
{
    auto name = "rep-" + std::to_string(session_id);
    m_future = tpool.async(name, [this]{
        run();
    });
}

void RepSession::stop()
{
    m_running.store(false, std::memory_order_relaxed);
}

RepSession::~RepSession()
{
    stop();
    m_future.get();
}

void RepSession::queue_query(QueryEvent&& qevent, int64_t commit_event_id)
{
    if (commit_event_id != -1)
    {
        mxb_assert(m_commit_event_id == -1);
        m_commit_event_id = commit_event_id;
    }

    std::lock_guard guard(m_mutex);
    m_queue.push_back(std::move(qevent));
    m_condition.notify_one();
}

void RepSession::run()
{
    if (ThisUnit::thread_idx == -1)
    {
        ThisUnit::thread_idx = ThisUnit::next_thread_idx.fetch_add(1, std::memory_order_relaxed);
    }

    m_pConn = mysql_init(nullptr);
    if (m_pConn == nullptr)
    {
        MXB_SERROR("Could not initialize connector-c " << mysql_error(m_pConn));
        exit(EXIT_FAILURE);
    }

    if (mysql_real_connect(m_pConn, m_config.host.address().c_str(), m_config.user.c_str(),
                           m_config.password.c_str(), "", m_config.host.port(), nullptr,
                           CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS) == nullptr)
    {
        MXB_SERROR("Could not connect to "
                   << m_config.host.address()
                   << ':' << std::to_string(m_config.host.port())
                   << " Error: " << mysql_error(m_pConn));
        exit(EXIT_FAILURE);
    }

    while (m_running.load(std::memory_order_relaxed))
    {
        std::unique_lock lock(m_mutex);
        m_condition.wait(lock, [this]{
            return !m_queue.empty();
        });

        auto qevent = std::move(m_queue.front());
        m_queue.pop_front();
        lock.unlock();

        if (is_session_close(qevent))
        {
            m_player.session_finished(*this);
            break;
        }
        else
        {
            execute_stmt(qevent);
            if (qevent.event_id == m_commit_event_id)
            {
                auto rep = m_commit_event_id;
                m_player.trxn_finished(rep);
            }
        }
    }

    mysql_close(m_pConn);
    m_player.session_finished(*this);
}
