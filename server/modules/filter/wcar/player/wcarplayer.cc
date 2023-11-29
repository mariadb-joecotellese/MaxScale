#include "../wcarsqlitestorage.hh"
#include "wcarplayerconfig.hh"
#include <maxsimd/canonical.hh>
#include <iostream>



bool execute_stmt(const PlayerConfig& config, const std::string& sql);
void replay(const PlayerConfig& config);

int main(int argc, char** argv)
{
    PlayerConfig config(argc, argv);

    replay(config);

    return EXIT_SUCCESS;
}

bool execute_stmt(const PlayerConfig& config, const std::string& sql)
{
    if (mysql_query(config.conn, sql.c_str()))
    {
        std::cerr << "MariaDB: Error code " << mysql_error(config.conn) << std::endl;
        return false;
    }

    while (MYSQL_RES* result = mysql_store_result(config.conn))
    {
        mysql_free_result(result);
    }

    return true;
}

void replay(const PlayerConfig& config)
{
    SqliteStorage storage(config.file_path, Access::READ_ONLY);

    for (const auto& event : storage)
    {
        auto sql = maxsimd::recreate_sql(event.canonical, event.canonical_args);
        std::cout << sql << std::endl;

        // TODO: Ignoring the return value until erranous queries are handled.
        // If the sql was in fact wrong during capture, "selct 42", the
        // same "selct 42" will run here.
        execute_stmt(config, sql);
    }
}
