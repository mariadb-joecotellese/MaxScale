#include "wcartransform.hh"
#include "../wcarsqlitestorage.hh"
#include "../wcarbooststorage.hh"
#include "maxbase/assert.hh"
#include <maxscale/parser.hh>

namespace fs = std::filesystem;

Transform::Transform(const PlayerConfig* pConfig)
    : m_config(*pConfig)
{
    std::cout << "Transform data for replay." << std::endl;
    maxbase::StopWatch sw;
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
        transform_events(*sStorage, *sBoost);
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
        transform_events(*sSqlite, *sStorage);
        // Reopen the boost file for reading
        sStorage = std::make_unique<BoostStorage>(path, ReadWrite::READ_ONLY);
    }

    m_player_storage = std::move(sStorage);

    std::cout << "Transform: " << mxb::to_string(sw.split()) << std::endl;
}

Storage& Transform::player_storage()
{
    return *m_player_storage;
}

// Expected transaction behavior
// begin and autocommit disable are both considered txn start

// autocommit is enabled (by default)
// begin                - trx start
// begin                - ignored
// update               - noted
// commit               - trx end
// commit               - ignored
// update               - a single stmt trx
// enable               - effectively a single stmt txn, but as it would affect
//                        other sessions in replay, not considered a txn.

// disable (autocommit) - trx start
// update               - noted
// commit or enable     - trx end, new trx start
// begin or enable      - no effect, already in a trx

// No attempt is made to delay the decision when a transaction starts based
// on the first write. So a read-only session that does a trx start
// becomes a transaction. If a session has an open transaction when it closes
// it is considered a txn (the session close adds a Transaction instance).

namespace
{
using namespace maxscale::sql;

// TODO is this complete?
constexpr auto WRITE_FLAGS = TYPE_WRITE | TYPE_READWRITE
    | TYPE_SESSION_WRITE | TYPE_USERVAR_WRITE | TYPE_GSYSVAR_WRITE;

/* The purpose of this class is to track the timeline of a session, and
 * create Transaction objects as the events are iterated over.
 */
class SessionState
{
public:
    SessionState(int64_t session_id)
        : m_session_id(session_id)
    {
    }

    // Returns a valid Transaction instance, if this was an end-of-txn.
    Transaction update(const QueryEvent& qevent)
    {
        Transaction txn;

        if (qevent.start_time == qevent.end_time)   // TODO make this a member fnct of QueryEvent
        {
            if (m_in_trx)
            {
                fill_txn(txn, qevent);
            }
        }
        else if (m_autocommit && (qevent.flags & TYPE_ENABLE_AUTOCOMMIT))
        {
            // pass, do not treat extra enables as single stmt txns
            MXB_SWARNING("EXTRA autommit enable " << qevent.event_id);
        }
        else
        {
            m_autocommit = (qevent.flags & TYPE_ENABLE_AUTOCOMMIT) ? true : m_autocommit;
            m_autocommit = (qevent.flags & TYPE_DISABLE_AUTOCOMMIT) ? false : m_autocommit;

            if (is_trx_start(qevent))
            {
                m_start_event_id = qevent.event_id;
                m_in_trx = !is_trx_end(qevent);     // For single stmt autocommit txns
            }

            if (is_trx_end(qevent))
            {
                m_in_trx = false;
                fill_txn(txn, qevent);
            }
        }

        return txn;
    }

private:
    bool is_trx_start(const QueryEvent& qevent)
    {
        return !m_in_trx
               && (qevent.flags & (TYPE_BEGIN_TRX | TYPE_DISABLE_AUTOCOMMIT)
                   || (m_autocommit == true && (qevent.flags & WRITE_FLAGS)));
    }

    bool is_trx_end(const QueryEvent& qevent)
    {
        // TYPE_READWRITE? and TYPE_NEXT_TRX?
        return (m_in_trx && (qevent.flags & (TYPE_COMMIT | TYPE_ROLLBACK | TYPE_ENABLE_AUTOCOMMIT)))
               || (!m_in_trx && m_autocommit && (qevent.flags & WRITE_FLAGS));
    }

    void fill_txn(Transaction& txn, const QueryEvent& qevent)
    {
        txn.session_id = m_session_id;
        txn.start_event_id = m_start_event_id;
        txn.end_event_id = qevent.event_id;
        txn.end_time = qevent.end_time;
    }

    int64_t m_session_id;
    int64_t m_start_event_id = -1;
    bool    m_in_trx = false;
    bool    m_autocommit = true;                // initially it is assumed autocommit is on
};
}

void Transform::transform_events(Storage& from, Storage& to)
{
    // Keyed by session_id.
    std::unordered_map<int64_t, SessionState> sessions;
    for (auto&& qevent : from)
    {
        auto session_ite = sessions.find(qevent.session_id);
        if (session_ite == end(sessions))
        {
            auto ins = sessions.emplace(qevent.session_id, SessionState {qevent.session_id});
            session_ite = ins.first;
        }

        SessionState& state = session_ite->second;

        auto trx = state.update(qevent);
        if (trx.is_valid())
        {
            m_trxs.push_back(trx);
        }

        to.add_query_event(std::move(qevent));
    }

    std::sort(begin(m_trxs), end(m_trxs), [](const Transaction& lhs, const Transaction& rhs){
        return lhs.end_time < rhs.end_time;
    });

    // Create the mappings
    for (auto ite = begin(m_trxs); ite != end(m_trxs); ++ite)
    {
        m_trx_start_mapping.emplace(ite->start_event_id, ite);
        m_trx_end_mapping.emplace(ite->end_event_id, ite);
    }
}
