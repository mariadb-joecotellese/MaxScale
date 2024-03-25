#include "reptransform.hh"
#include "../capbooststorage.hh"
#include "repbooststorage.hh"
#include "repcsvstorage.hh"
#include "maxbase/assert.hh"
#include <maxbase/json.hh>
#include <maxscale/parser.hh>
#include <execution>

RepTransform::RepTransform(const RepConfig* pConfig, Action action)
    : m_config(*pConfig)
{
    MXB_SNOTICE("Transform data for replay.");

    maxbase::StopWatch sw;
    fs::path path{m_config.file_name};
    auto ext = path.extension();

    if (ext != ".cx")
    {
        MXB_THROW(WcarError, "The replay file must be binary, extension 'cx', got " << path);
    }

    // Transform
    transform_events(path, action);

    // Open files for reading.
    m_player_storage = std::make_unique<CapBoostStorage>(path, ReadWrite::READ_ONLY);

    if (action == Action::REPLAY)
    {
        // Open for writing. Only rep events will be written.
        m_rep_event_storage = m_config.build_rep_storage();
    }

    MXB_SNOTICE("Transform: " << mxb::to_string(sw.split()));
}

void RepTransform::finalize()
{
    m_player_storage.reset();
    m_rep_event_storage.reset();
}

void RepTransform::transform_events(const fs::path& path, Action action)
{
    auto tx_path = path;
    tx_path.replace_extension("tx");
    bool needs_sorting = !fs::exists(tx_path);

    CapBoostStorage boost{path, ReadWrite::READ_ONLY};
    CapBoostStorage::SortReport report;

    mxb::StopWatch sw;
    int num_active_session = 0;
    m_max_parallel_sessions = 0;
    int num_sessions = 0;
    std::unordered_set<int64_t> session_ids;

    if (needs_sorting)
    {
        // Calculate m_max_parallel_sessions
        auto sort_cb = [&](const QueryEvent& qevent){
            auto session_ite = session_ids.find(qevent.session_id);
            if (session_ite == end(session_ids))
            {
                session_ids.insert(qevent.session_id);
                ++num_sessions;
                ++num_active_session;
                m_max_parallel_sessions = std::max(num_active_session, m_max_parallel_sessions);
            }
            else if (is_session_close(qevent))
            {
                session_ids.erase(qevent.session_id);
                --num_active_session;
            }
        };

        report = boost.sort_query_event_file(sort_cb);
    }

    m_trxs = boost.release_trx_events();

    // Create the mappings
    for (auto ite = begin(m_trxs); ite != end(m_trxs); ++ite)
    {
        m_trx_start_mapping.emplace(ite->start_event_id, ite);
        m_trx_end_mapping.emplace(ite->end_event_id, ite);
    }

    mxb::Json tx_js;

    if (needs_sorting)
    {
        mxb::Json capture(mxb::Json::Type::OBJECT);
        capture.set_real("duration", mxb::to_secs(report.capture_duration));
        capture.set_int("events", report.events);
        capture.set_int("transactions", m_trxs.size());
        capture.set_int("sessions", num_sessions);
        capture.set_int("max_parallel_sessions", m_max_parallel_sessions);

        mxb::Json transform(mxb::Json::Type::OBJECT);
        transform.set_real("read", mxb::to_secs(report.read));
        transform.set_real("sort", mxb::to_secs(report.sort));
        transform.set_real("write", mxb::to_secs(report.write));

        mxb::Json js(mxb::Json::Type::OBJECT);
        js.set_real("duration", mxb::to_secs(sw.split()));
        js.set_object("capture", std::move(capture));
        js.set_object("transform_steps", std::move(transform));

        std::ofstream tx_file(tx_path);
        tx_file << js.to_string(mxb::Json::Format::PRETTY) << std::endl;
        tx_js = std::move(js);
    }
    else
    {
        tx_js.load(tx_path.string());
        m_max_parallel_sessions = tx_js.at("capture/max_parallel_sessions").get_int();
        mxb_assert(m_max_parallel_sessions > 0);
    }

    MXB_SNOTICE("Original sort time: " << tx_js.at("duration").get_real() << "s");
    MXB_SNOTICE("Events: " << tx_js.at("capture/events").to_string(mxb::Json::Format::COMPACT));
    MXB_SNOTICE("Transactions: " << tx_js.at("capture/transactions").to_string(mxb::Json::Format::COMPACT));
    MXB_SNOTICE("Sessions: " << tx_js.at("capture/sessions").to_string(mxb::Json::Format::COMPACT));
    MXB_SNOTICE("Expected runtime: " << tx_js.at("capture/duration").get_real() << "s");
}
