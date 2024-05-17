#include "repsession.hh"
#include "repplayer.hh"
#include "../simtime.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <maxbase/string.hh>
#include <maxsimd/canonical.hh>
#include <maxsql/mariadb_connector.hh>
#include <iostream>
#include <thread>

namespace
{

struct ThreadInfo
{
    uint64_t               session_id {0};
    unsigned int           thread_id {0};
    bool                   executing {false};
    int64_t                last_event_id {-1};
    mxb::Clock::time_point last_event_ts {mxb::Duration{0}};
};

namespace ThisUnit
{
std::atomic<int> next_thread_idx = 0;
thread_local int thread_idx = -1;

// Deadlock monitor thread
std::vector<ThreadInfo> infos;
std::mutex lock;
std::condition_variable cv;
std::atomic<bool> monitor_running {false};
std::thread monitor_thr;
}

void dump_infos()
{
    std::lock_guard guard(ThisUnit::lock);

    for (auto& t : ThisUnit::infos)
    {
        std::cout
            << "Session: " << t.session_id << ", "
            << "Thread: " << t.thread_id << ", "
            << "Event ID: " << t.last_event_id << ", "
            << "Time since start: " << mxb::to_string(mxb::Clock::now() - t.last_event_ts)
            << std::endl;
    }
}

ThreadInfo* find_session(unsigned int thread_id)
{
    for (size_t i = 0; i < ThisUnit::infos.size(); i++)
    {
        if (ThisUnit::infos[i].thread_id == thread_id)
        {
            return &ThisUnit::infos[i];
        }
    }

    return nullptr;
}

void deadlock_monitor(std::string user, std::string password, std::string address, int port)
{
    mxq::MariaDB conn;
    auto& s = conn.connection_settings();
    s.user = user;
    s.password = password;

    if (!conn.open(address, port))
    {
        MXB_SERROR("Could not connect to " << address << ':' << std::to_string(port)
                                           << " Error: " << conn.error());
        return;
    }

    auto res = conn.query("SELECT @@global.innodb_lock_wait_timeout");
    res->next_row();
    std::chrono::seconds lock_wait_timeout {res->get_int(0)};
    res.reset();

    while (ThisUnit::monitor_running)
    {
        res = conn.query("SHOW ENGINE INNODB STATUS");

        if (res && res->next_row() && res->get_col_count() >= 3)
        {
            std::istringstream iss(res->get_string(2));
            unsigned int thread_id = 0;
            const std::string_view thr_prefix = "MariaDB thread id ";
            const std::string_view lock_wait_prefix = "TRX HAS BEEN WAITING ";

            for (std::string line; std::getline(iss, line);)
            {
                if (auto pos = line.find(thr_prefix); pos != std::string::npos)
                {
                    thread_id = strtoul(line.c_str() + pos + thr_prefix.size(), nullptr, 10);
                }

                if (auto pos = line.find(lock_wait_prefix); pos != std::string::npos)
                {
                    std::chrono::microseconds wait_usec {
                        strtoul(line.c_str() + pos + lock_wait_prefix.size(), nullptr, 10)
                    };

                    if (wait_usec > lock_wait_timeout * 0.75)
                    {
                        if (auto* info = find_session(thread_id))
                        {
                            std::cout << "Session " << info->session_id << " has been stuck over "
                                      << mxb::to_string(wait_usec) << " on event " << info->last_event_id
                                      << ". Connection ID: " << info->thread_id << std::endl;

                            dump_infos();
                        }
                    }
                }
            }
        }

        std::unique_lock guard(ThisUnit::lock);
        ThisUnit::cv.wait_for(guard, 5s, [&](){
            return !ThisUnit::monitor_running;
        });
    }
}
}

void start_deadlock_monitor(int max_sessions,
                            std::string user, std::string password,
                            std::string address, int port)
{
    ThisUnit::infos.resize(max_sessions);
    ThisUnit::monitor_running = true;
    ThisUnit::monitor_thr = std::thread(deadlock_monitor, user, password, address, port);
}

void stop_deadlock_monitor()
{
    ThisUnit::monitor_running = false;
    ThisUnit::cv.notify_one();
    ThisUnit::monitor_thr.join();
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
    revent.start_time = SimTime::sim_time().real_now();
    revent.num_rows = 0;
    revent.rows_read = 0;
    revent.error = 0;

    int orig_err = get_error(qevent);
    int rc = 0;

    if (qevent.flags & CAP_PING)
    {
        rc = mysql_ping(m_pConn);
    }
    else if (qevent.flags & CAP_RESET_CONNECTION)
    {
        rc = mysql_reset_connection(m_pConn);
    }
    else
    {
        rc = mysql_query(m_pConn, sql.c_str());
    }

    if (rc)
    {
        int error_number = mysql_errno(m_pConn);

        if (orig_err != error_number)
        {
            MXB_SERROR("MariaDB: Error S "
                       << qevent.session_id << " E " << qevent.event_id
                       << " SQL " << mxb::show_some(sql)
                       << " Error code " << error_number << ": " << mysql_error(m_pConn));
        }

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

    revent.end_time = SimTime::sim_time().real_now();

    if (is_real_event(qevent))
    {
        m_pRecorder->get_shared_data_by_index(ThisUnit::thread_idx)->send_update(std::move(revent));
    }

    // If the query ended with a ER_LOCK_DEADLOCK error, the server rolled back the transaction automatically.
    // If the query succeeded in the replay without a deadlock, we'll have to roll it back manually to make
    // sure that any locks held by the transaction are not left open. This'll make sure that transactions that
    // were automatically rolled back do not end up blocking transactions that did not get rolled back in the
    // capture.
    const int ER_LOCK_DEADLOCK = 1213;

    if (orig_err == ER_LOCK_DEADLOCK && revent.error != ER_LOCK_DEADLOCK)
    {
        mysql_query(m_pConn, "ROLLBACK");
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

    auto& info = ThisUnit::infos[ThisUnit::thread_idx];
    info.thread_id = mysql_thread_id(m_pConn);
    info.last_event_ts = mxb::Clock::now();

    while (m_running.load(std::memory_order_relaxed))
    {
        std::unique_lock lock(m_mutex);
        m_condition.wait(lock, [this]{
            return !m_queue.empty();
        });

        auto qevent = std::move(m_queue.front());

        if (info.session_id == 0)
        {
            info.session_id = qevent.session_id;
        }

        info.last_event_id = qevent.event_id;
        m_queue.pop_front();
        lock.unlock();

        if (is_session_close(qevent))
        {
            m_player.session_finished(*this);
            break;
        }
        else
        {
            // Only mark sessions inside a transaction as executing. This avoids long-running commands like
            // ALTER TABLE from being falsely reported as deadlocked.
            info.executing = in_trxn();
            info.last_event_ts = mxb::Clock::now();
            execute_stmt(qevent);
            info.executing = false;
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
