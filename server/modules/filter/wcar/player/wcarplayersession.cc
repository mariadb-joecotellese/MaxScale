#include "wcarplayersession.hh"
#include "wcarplayer.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <maxbase/string.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <thread>

bool execute_stmt(MYSQL* pConn, const QueryEvent& qevent)
{
    auto sql = maxsimd::recreate_sql(*qevent.sCanonical, qevent.canonical_args);

    if (mysql_query(pConn, sql.c_str()))
    {
        std::cerr << "MariaDB: Error S " << qevent.session_id << " E " << qevent.event_id
                  << " SQL " << mxb::show_some(sql)
                  << " Error code " << mysql_error(pConn) << std::endl;
        return false;
    }

    while (MYSQL_RES* result = mysql_store_result(pConn))
    {
        mysql_free_result(result);
    }

    return true;
}

PlayerSession::PlayerSession(const PlayerConfig* pConfig, Player* pPlayer, int64_t session_id)
    : m_config(*pConfig)
    , m_player(*pPlayer)
    , m_session_id(session_id)
    , m_thread(&PlayerSession::run, this)
{
}

PlayerSession::~PlayerSession()
{
    mxb_assert(m_queue.empty());
    m_thread.join();
}

void PlayerSession::queue_query(QueryEvent&& qevent, int64_t commit_event_id)
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

void PlayerSession::run()
{
    m_pConn = mysql_init(nullptr);
    if (m_pConn == nullptr)
    {
        std::cerr << "Could not initialize connector-c " << mysql_error(m_pConn) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (mysql_real_connect(m_pConn, m_config.host.address().c_str(), m_config.user.c_str(),
                           m_config.password.c_str(), "", m_config.host.port(), nullptr, 0) == nullptr)
    {
        std::cerr << "Could not connect to " << m_config.host.address()
                  << ':' << std::to_string(m_config.host.port())
                  << " Error: " << mysql_error(m_pConn) << '\n';
        exit(EXIT_FAILURE);
    }

    for (;;)
    {
        std::unique_lock lock(m_mutex);
        m_condition.wait(lock, [this]{
            return !m_queue.empty();
        });

        auto qevent = std::move(m_queue.front());
        m_queue.pop_front();
        lock.unlock();

        if (qevent.start_time == qevent.end_time)
        {
            m_player.session_finished(*this);
            break;
        }
        else
        {
            execute_stmt(m_pConn, qevent);
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
