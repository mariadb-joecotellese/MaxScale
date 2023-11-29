#include <maxbase/host.hh>
#include <mysql.h>
#include <string>

struct PlayerConfig
{
    PlayerConfig(int argc, char** argv);

    std::string   user{"maxskysql"};
    std::string   password{"skysql"};
    maxbase::Host host{"127.1.1.0", 3306};

    std::string file_path;
    MYSQL*      conn;

    void show_help();
};
