#include "wcarplayer.hh"
#include "wcarplayerconfig.hh"
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <iostream>

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

Player::Player(const PlayerConfig* pConfig)
    : m_config(*pConfig)
{
}

void Player::replay()
{
    mxb::StopWatch sw;
    auto path = m_config.capture_dir + '/' + m_config.file_base_name;
    auto sStorage = m_config.create_read_storage(path);

    int64_t count = 0;

    for (const auto& event : *sStorage)
    {
        auto sql = maxsimd::canonical_args_to_sql(*event.sCanonical, event.canonical_args);
//        std::cout << "sql = " << sql << std::endl;
        if ((++count % 251) == 0)
        {
            std::cout << "\r" << count << std::flush;
        }

        // TODO: Ignoring the return value until erranous queries are handled.
        // If the sql was in fact wrong during capture, "selct 42", the
        // same "selct 42" will run here.
        execute_stmt(m_config.pConn, sql);
    }

    std::cout << "\r" << count << std::endl;
}
