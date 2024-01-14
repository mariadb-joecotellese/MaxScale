#include "wcartransform.hh"
#include "../wcarsqlitestorage.hh"
#include "../wcarbooststorage.hh"

namespace fs = std::filesystem;

Transform::Transform(const PlayerConfig* pConfig)
    : m_config(*pConfig)
{
    fs::path path = m_config.capture_dir + '/' + m_config.file_base_name;
    auto ext = path.extension();

    if (ext == ".sqlite")
    {
        m_player_storage = std::make_unique<SqliteStorage>(path, Access::READ_ONLY);
    }
    else if (ext == ".cx" || ext == ".ex")
    {
        m_player_storage = std::make_unique<BoostStorage>(path, ReadWrite::READ_ONLY);
    }
    else
    {
        abort();
    }
}

Storage& Transform::player_storage()
{
    return *m_player_storage;
}
