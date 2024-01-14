#include "wcarplayersession.hh"
#include "wcarplayer.hh"
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <thread>

bool execute_stmt(MYSQL* pConn, const std::string& sql)
{
    if (mysql_query(pConn, sql.c_str()))
    {
        std::cerr << "MariaDB: Error code " << mysql_error(pConn) << std::endl;
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
}

PlayerSession::~PlayerSession()
{
    mysql_close(m_pConn);
}

void PlayerSession::queue_query(const std::string& sql)
{
    execute_stmt(m_pConn, sql);
}
