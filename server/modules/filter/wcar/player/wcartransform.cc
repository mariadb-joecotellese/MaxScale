#include "wcartransform.hh"
#include "../wcarsqlitestorage.hh"
#include "../wcarbooststorage.hh"

namespace fs = std::filesystem;

Transform::Transform(const PlayerConfig* pConfig)
    : m_config(*pConfig)
{
    StorageType stype;
    fs::path path = m_config.capture_dir + '/' + m_config.file_base_name;
    auto ext = path.extension();
    std::unique_ptr<Storage> sStorage;

    if (ext == ".sqlite")
    {
        stype = StorageType::SQLITE;
        sStorage = std::make_unique<SqliteStorage>(path, Access::READ_ONLY);
    }
    else if (ext == ".cx" || ext == ".ex")
    {
        stype = StorageType::BINARY;
        sStorage = std::make_unique<BoostStorage>(path, ReadWrite::READ_ONLY);
    }
    else
    {
        MXB_THROW(WcarError, "Unknown extension " << ext);
    }

    if (stype == StorageType::SQLITE)
    {
        // Copy sqlite storage to new boost storage instance, sorting by start_time
        fs::path boost_path{path};
        boost_path.replace_extension(".cx");
        auto sBoost = std::make_unique<BoostStorage>(boost_path, ReadWrite::WRITE_ONLY);
        static_cast<SqliteStorage*>(sStorage.get())->set_sort_by_start_time();
        sBoost->move_values_from(*sStorage);
        sBoost.reset();
        // Reopen the boost file for reading
        sStorage = std::make_unique<BoostStorage>(boost_path, ReadWrite::READ_ONLY);
    }
    else
    {
        // Copy from existing storage to a sqlite storage
        fs::path sqlite_path{path};
        sqlite_path.replace_extension(".sqlite");
        std::unique_ptr<SqliteStorage> sSqlite = std::make_unique<SqliteStorage>(sqlite_path,
                                                                                 Access::READ_WRITE);
        sSqlite->move_values_from(*sStorage);

        // Delete the original boost files (could backup instead).
        // TODO: Clunky, the storage should do it. There will be at least one more file.
        auto delete_path = path;
        delete_path.replace_extension(".cx");
        fs::remove(delete_path);
        delete_path.replace_extension(".ex");
        fs::remove(delete_path);

        // Copy from sqlite to a new boost storage sorted by start_time
        sSqlite->set_sort_by_start_time();
        sStorage = std::make_unique<BoostStorage>(path, ReadWrite::WRITE_ONLY);
        sStorage->move_values_from(*sSqlite);
        // Reopen the boost file for reading
        sStorage = std::make_unique<BoostStorage>(path, ReadWrite::READ_ONLY);
    }

    m_player_storage = std::move(sStorage);
}

Storage& Transform::player_storage()
{
    return *m_player_storage;
}
