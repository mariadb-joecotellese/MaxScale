#include "reptransform.hh"
#include "../capsqlitestorage.hh"
#include "../capbooststorage.hh"
#include "maxbase/assert.hh"
#include <maxscale/parser.hh>

namespace fs = std::filesystem;

RepTransform::RepTransform(const RepConfig* pConfig)
    : m_config(*pConfig)
{
    std::cout << "Transform data for replay." << std::endl;

    maxbase::StopWatch sw;
    fs::path path{m_config.file_name};
    auto ext = path.extension();

    // The storage given to the player can be of any kind, but is currently fixed.
    // The sqlite sorting appears to be too slow. Writing to sqlite is fast, so maybe
    // there could still be a way to use sqlite (investigate), otherwise boost storage
    // will have to become sortable.
    // m_player_storage  a boost storage to read from.
    // rep_event_storage a sqlite storage to write RepEvents to.
    //                   The table rep_event in this storage is truncated for replay.
    fs::path sqlite_path{path};
    std::unique_ptr<CapSqliteStorage> sSqlite;

    if (ext == ".sqlite")
    {
        sSqlite = std::make_unique<CapSqliteStorage>(sqlite_path, Access::READ_WRITE);
    }
    else if (ext == ".cx" || ext == ".ex")
    {
        sqlite_path.replace_extension(".sqlite");

        CapBoostStorage boost{path, ReadWrite::READ_ONLY};
        sSqlite = std::make_unique<CapSqliteStorage>(sqlite_path, Access::READ_WRITE);
        sSqlite->move_values_from(boost);
    }
    else
    {
        MXB_THROW(WcarError, "Unknown extension " << ext);
    }

    // New boost storage to transform into. TODO: In the `else`
    // above the original boost files should be deleted (maybe),
    // but instead new files are created to make testing easier.
    fs::path boost_path{path};
    boost_path.replace_extension("");
    boost_path.replace_filename(boost_path.filename().string() + "-replay");
    boost_path.replace_extension(".cx");

    m_player_storage = std::make_unique<CapBoostStorage>(boost_path, ReadWrite::WRITE_ONLY);

    // Transform
    sSqlite->truncate_rep_events();
    sSqlite->set_sort_by_start_time();
    transform_events(*sSqlite, *m_player_storage);

    // Reopen sqlite. TODO: Storage::reset(), preserve caching.
    sSqlite.reset();
    m_rep_event_storage = std::make_unique<CapSqliteStorage>(sqlite_path, Access::READ_WRITE);

    // Reopen the boost file for reading. TODO: Storage::reset(), preserve caching.
    m_player_storage.reset();
    m_player_storage = std::make_unique<CapBoostStorage>(boost_path, ReadWrite::READ_ONLY);

    std::cout << "Transform: " << mxb::to_string(sw.split()) << std::endl;
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
    explicit SessionState(int64_t session_id)
        : m_session_id(session_id)
    {
    }

    // Returns a valid Transaction instance, if this was an end-of-txn.
    Transaction update(const QueryEvent& qevent)
    {
        Transaction txn;

        if (qevent.is_session_close())
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

void RepTransform::transform_events(Storage& from, Storage& to)
{
    int num_active_session = 0;
    m_max_parallel_sessions = 0;

    // Keyed by session_id.
    std::unordered_map<int64_t, SessionState> sessions;
    for (auto&& qevent : from)
    {
        auto session_ite = sessions.find(qevent.session_id);
        if (session_ite == end(sessions))
        {
            ++num_active_session;
            m_max_parallel_sessions = std::max(num_active_session, m_max_parallel_sessions);
            auto ins = sessions.emplace(qevent.session_id, SessionState {qevent.session_id});
            session_ite = ins.first;
        }
        else if (qevent.is_session_close())
        {
            --num_active_session;
        }

        SessionState& state = session_ite->second;

        auto trx = state.update(qevent);
        if (trx.is_valid())
        {
            m_trxs.push_back(trx);
        }

        to.add_query_event(std::move(qevent));
    }

    std::cout << "max_parallel_sessions " << m_max_parallel_sessions << std::endl;

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
