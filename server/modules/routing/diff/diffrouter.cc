/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffrouter.hh"
#include <cmath>
#include <fstream>
#include <iomanip>
#include <maxbase/format.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/protocol/mariadb/gtid.hh>
#include <maxscale/routingworker.hh>
#include <maxsql/mariadb_connector.hh>
#include "../../../core/internal/config.hh"
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/service.hh"
#include "diffroutersession.hh"
#include "diffutils.hh"

using namespace mxq;
using namespace mxs;
using std::chrono::duration_cast;


DiffRouter::DiffRouter(SERVICE* pService)
    : mxb::Worker::Callable(mxs::MainWorker::get())
    , m_config(pService->name(), this)
    , m_service(*pService)
    , m_stats(m_config.qps_window)
    , m_sHSRegistry(std::make_shared<HSRegistry>())
{
}

DiffRouter::~DiffRouter()
{
    summary(Summary::SAVE, nullptr);
}

// static
const char* DiffRouter::to_string(DiffState diff_state)
{
    switch (diff_state)
    {
    case DiffState::PREPARED:
        return "prepared";

    case DiffState::SYNCHRONIZING:
        return "synchronizing";

    case DiffState::COMPARING:
        return "comparing";

    case DiffState::STOPPING:
        return "stopping";
    }

    mxb_assert(!true);
    return "unknown";
}

// static
const char* DiffRouter::to_string(SyncState sync_state)
{
    switch (sync_state)
    {
    case SyncState::NOT_APPLICABLE:
        return "not_applicable";

    case SyncState::STOPPING_REPLICATION:
        return "stopping_replication";

    case SyncState::SUSPENDING_SESSIONS:
        return "suspending_sessions";

    }

    mxb_assert(!true);
    return "unknown";
}

// static
DiffRouter* DiffRouter::create(SERVICE* pService)
{
    return new DiffRouter(pService);
}

std::shared_ptr<mxs::RouterSession> DiffRouter::newSession(MXS_SESSION* pSession, const Endpoints& endpoints)
{
    const auto& children = m_service.get_children();

    if (std::find(children.begin(), children.end(), m_config.pMain) == children.end())
    {
        MXB_ERROR("Main target '%s' is not listed in `targets`", m_config.pMain->name());
        return nullptr;
    }

    auto [sMain, backends] = diff::backends_from_endpoints(*m_config.pMain, endpoints, *this);
    bool connected = false;

    if (sMain->can_connect() && sMain->connect())
    {
        connected = true;

        for (const auto& sBackend : backends)
        {
            // We do not call can_connect(), but simply attempt to connect. That
            // removes the need for having it monitored. Further, there's nothing
            // we can do if we cannot connect or if something fails later.
            sBackend->connect();
        }
    }

    std::shared_ptr<mxs::RouterSession> sRouter_session;

    if (connected)
    {
        sRouter_session = std::make_shared<DiffRouterSession>(pSession, this,
                                                              std::move(sMain), std::move(backends));
    }

    return sRouter_session;
}

std::shared_ptr<DiffExporter> DiffRouter::exporter_for(const mxs::Target* pTarget) const
{
    std::shared_lock<std::shared_mutex> guard(m_exporters_rwlock);

    auto it = m_exporters.find(pTarget);
    mxb_assert(it != m_exporters.end());

    return it->second;
}

json_t* DiffRouter::diagnostics() const
{
    return nullptr;
}

uint64_t DiffRouter::getCapabilities() const
{
    return DIFF_CAPABILITIES;
}

bool DiffRouter::post_configure()
{
    bool rv= false;

    auto targets = m_service.get_children();

    if (targets.size() == 2) // TODO: Allow multiple concurrent diffs at a later stage.
    {
        rv = true;

        for (const mxs::Target* pTarget : m_service.get_children())
        {
            if (pTarget->kind() != mxs::Target::Kind::SERVER)
            {
                MXB_ERROR("The target '%s' is not a server. Only servers may be "
                          "used as targets of '%s'.",
                          pTarget->name(), m_service.name());
                rv = false;
            }
        }

        if (rv)
        {
            auto end = targets.end();
            if (std::find(targets.begin(), end, m_config.pMain) != end)
            {
                rv = update_exporters();

                if (rv)
                {
                    m_registry.set_max_entries(m_config.entries);
                    m_registry.set_period(m_config.period);

                    m_stats.post_configure(m_config);
                }
            }
            else
            {
                MXB_ERROR("The value of 'main' (%s) is not one of the servers in 'targets'.",
                          m_config.pMain->name());
            }
        }
    }
    else
    {
        MXB_ERROR("'%s' needs exactly two servers as targets.", m_service.name());
    }

    return rv;
}

bool DiffRouter::check_configuration()
{
    // Called at MaxScale startup.

    bool rv = true;

    mxb_assert(m_config.pService);

    m_start_replication.clear();

    const std::vector<mxs::Target*>& targets = m_config.pService->get_children();

    auto it = std::find(targets.begin(), targets.end(), &m_service);

    if (it != targets.end())
    {
        // We seem to be a direct child of the service.

        const auto& sConfig = m_service.config();
        auto user = sConfig->user;
        auto password = sConfig->password;

        auto* pMain = m_config.pMain;
        std::optional<ReplicationInfo> ri_main = get_replication_info(*pMain, user, password);

        if (ri_main)
        {
            for (mxs::Target* pTarget : m_service.get_children())
            {
                if (pTarget == pMain)
                {
                    continue;
                }

                mxb_assert(pTarget->kind() == mxs::Target::Kind::SERVER);
                SERVER* pOther = static_cast<SERVER*>(pTarget);

                auto ri_other = get_replication_info(*pOther, user, password);

                if (ri_other)
                {
                    if (ri_other->will_replicate_from(*ri_main))
                    {
                        if (ri_other->is_currently_replicating())
                        {
                            MXB_ERROR("'%s' is target of '%s', but other '%s' is currently "
                                      "replicating from main '%s'. Cannot continue.",
                                      m_service.name(), m_config.pService->name(),
                                      pOther->name(), pMain->name());
                            rv = false;
                        }
                        else
                        {
                            m_start_replication.push_back(pOther);
                        }
                    }
                    else if (ri_other->has_same_master(*ri_main))
                    {
                        if (ri_other->is_currently_replicating() != ri_main->is_currently_replicating())
                        {
                            MXB_ERROR("Main '%s' and other '%s' are configured to replicate from the "
                                      "same server at %s:%d, but one of them is replicating and "
                                      "the other one is not. Cannot continue.",
                                      pMain->name(), pOther->name(),
                                      ri_other->master_host.c_str(),
                                      ri_other->master_port);
                            rv = false;
                        }
                    }
                    else
                    {
                        MXB_ERROR("Cannot figure out the relationship between main '%s' and "
                                  "other '%s'. Cannot continue.",
                                  pMain->name(), pOther->name());
                        rv = false;
                    }
                }
                else
                {
                    rv = false;
                }

                if (!rv)
                {
                    break;
                }
            }

            if (rv)
            {
                m_diff_state = DiffState::COMPARING;
                m_sync_state = SyncState::NOT_APPLICABLE;
            }
            else
            {
                m_start_replication.clear();
            }
        }
        else
        {
            rv = false;
        }
    }
    else
    {
        it = std::find(targets.begin(), targets.end(), m_config.pMain);

        if (it != targets.end())
        {
            // Main found where it is supposed to be. So, we are prepared
            // and must be started before comparing is done.
            m_diff_state = DiffState::PREPARED;
            m_sync_state = SyncState::NOT_APPLICABLE;

            MXB_NOTICE("'%s' starting in the '%s' state. Must be started "
                       "in order for the comparison to proceed.",
                       m_service.name(), to_string(m_diff_state));
        }
        else
        {
            const auto* zMy_name = m_service.name();
            const auto* zHis_name = m_config.pService->name();

            MXB_ERROR("Not able to figure out in what state '%s' should start up "
                      "in. '%s' should either be a target of '%s', or the main "
                      "server of '%s' should be a target of '%s'.",
                      zMy_name, zMy_name, zHis_name, zMy_name, zHis_name);
            rv = false;
        }
    }

    return rv;
}

bool DiffRouter::start(json_t** ppOutput)
{
    mxb_assert(MainWorker::is_current());

    if (m_diff_state != DiffState::PREPARED)
    {
        MXB_ERROR("State of '%s' is '%s'. Can be started only when in state '%s'.",
                  m_service.name(), to_string(m_diff_state), to_string(DiffState::PREPARED));
        return false;
    }

    set_state(DiffState::SYNCHRONIZING, SyncState::SUSPENDING_SESSIONS);

    RoutingWorker::SessionResult sr = suspend_sessions();

    MainWorker::get()->lcall([this, sr]() {
            if (collect_servers_to_be_stopped())
            {
                setup(sr);

                if (m_diff_state == DiffState::SYNCHRONIZING)
                {
                    start_setup_dcall();
                }
            }
            else
            {
                set_state(DiffState::PREPARED);
            }
        });

    get_status(sr, ppOutput);

    return true;
}

bool DiffRouter::status(json_t** ppOutput)
{
    RoutingWorker::SessionResult sr = suspended_sessions();

    get_status(sr, ppOutput);

    return true;
}

bool DiffRouter::stop(json_t** ppOutput)
{
    mxb_assert(MainWorker::is_current());

    bool rv = false;

    switch (m_diff_state)
    {
    case DiffState::PREPARED:
        MXB_ERROR("The state of '%s' is '%s' and hence it cannot be stopped.",
                  m_service.name(), to_string(m_diff_state));
        break;

    case DiffState::SYNCHRONIZING:
        mxb_assert(m_dcstart != 0);
        cancel_dcall(m_dcstart);
        m_dcstart = 0;

        resume_sessions();

        set_state(DiffState::PREPARED);
        rv = true;
        break;

    case DiffState::STOPPING:
        MXB_ERROR("'%s' is already being stopped.", m_service.name());
        break;

    case DiffState::COMPARING:
        {
            set_state(DiffState::STOPPING, SyncState::SUSPENDING_SESSIONS);

            RoutingWorker::SessionResult sr = suspend_sessions();

            MainWorker::get()->lcall([this, sr]() {
                    teardown(sr);

                    if (m_diff_state == DiffState::STOPPING)
                    {
                        start_teardown_dcall();
                    }
                });

            get_status(sr, ppOutput);
            rv = true;
        }
    }

    return rv;
}

namespace
{

bool save_stats(const std::string& path, json_t* pOutput)
{
    std::ofstream out(path);

    if (out)
    {
        auto str = mxb::json_dump(pOutput, JSON_INDENT(2)) + '\n';

        out << str;

        if (!out)
        {
            MXB_ERROR("Could not write summary to file '%s'.", path.c_str());
        }
    }
    else
    {
        MXB_ERROR("Could not create file '%s'.", path.c_str());
    }

    return !out.fail();
}

}

bool DiffRouter::summary(Summary summary, json_t** ppOutput)
{
    bool rv = true;

    std::unique_lock<std::mutex> guard(m_stats_lock);
    Stats stats = m_stats;
    guard.unlock();

    std::string base = mxs::datadir();
    base += "/";
    base += MXB_MODULE_NAME;
    base += "/";
    base += m_config.service_name;
    base += "/Summary_";

    time_t now = time(nullptr);
    std::stringstream time;
    time << std::put_time(std::localtime(&now),"%Y-%m-%d_%H%M%S");

    json_t* pOutput = should_return(summary) ? json_object() : nullptr;
    json_t* pMain = nullptr;
    json_t* pOthers = nullptr;

    if (pOutput)
    {
        pMain = json_object();
        pOthers = json_object();

        json_object_set_new(pOutput, "main", pMain);
        json_object_set_new(pOutput, "others", pOthers);
    }

    std::map<mxs::Target*, json_t*> jsons_by_target = stats.get_jsons();

    for (const auto& kv : jsons_by_target)
    {
        std::string s = base + kv.first->name() + "_" + time.str() + ".json";

        save_stats(s, kv.second);

        if (pOutput)
        {
            mxb_assert(pMain && pOthers);

            if (kv.first == m_config.pMain)
            {
                json_object_set_new(pMain, kv.first->name(), kv.second);
            }
            else
            {
                json_object_set_new(pOthers,  kv.first->name(), kv.second);
            }
        }
        else
        {
            json_decref(kv.second);
        }
    }

    if (pOutput)
    {
        *ppOutput = pOutput;
    }

    return rv;
}

void DiffRouter::collect(const DiffRouterSessionStats& stats)
{
    std::lock_guard<std::mutex> guard(m_stats_lock);

    m_stats.add(stats, m_config);
}

namespace
{

// https://en.wikipedia.org/wiki/Histogram#Freedman%E2%80%93Diaconis'_choice
inline mxb::Duration calculate_delta_fd(const std::vector<mxb::Duration>& samples, size_t size)
{
    mxb_assert(size <= samples.size());

    auto iqr = samples[size * 0.75] - samples[size * 0.25];
    auto delta = 2 * iqr / std::cbrt(size);

    return duration_cast<mxb::Duration>(delta);
}

// https://en.wikipedia.org/wiki/Histogram#Sturges'_formula
inline mxb::Duration calculate_delta_sturges(mxb::Duration min, mxb::Duration max, size_t size)
{
    auto delta = (max - min) / (log2(size) + 1);

    return duration_cast<mxb::Duration>(delta);
}

}

std::shared_ptr<const DiffRouter::HSRegistry> DiffRouter::add_sample_for(std::string_view canonical,
                                                                         const mxb::Duration& duration)
{
    std::shared_ptr<const HSRegistry> sHSRegistry;

    std::shared_lock<std::shared_mutex> shared_guard(m_hsregistry_rwlock);

    if (m_sHSRegistry->find(canonical) != m_sHSRegistry->end())
    {
        // The current m_sHSRegistry contains the bin specification for
        // the canonical statement, so return it.
        sHSRegistry = m_sHSRegistry;
    }
    else
    {
        shared_guard.unlock();

        std::lock_guard<std::shared_mutex> guard(m_hsregistry_rwlock);

        // Have to check again; something might have happened between the shared
        // guard being unlocked and the guard being locked.

        if (m_sHSRegistry->find(canonical) != m_sHSRegistry->end())
        {
            sHSRegistry = m_sHSRegistry;
        }
        else
        {
            auto it = m_samples_by_canonical.find(canonical);

            size_t nRequired_samples = m_config.samples;

            if (it == m_samples_by_canonical.end())
            {
                // First sample, so add an entry.
                it = m_samples_by_canonical.emplace(std::string(canonical), Samples()).first;
                it->second.reserve(nRequired_samples);
            }

            Samples& samples = it->second;
            samples.push_back(duration);

            if (samples.size() >= nRequired_samples)
            {
                // Enough samples collected.
                std::sort(samples.begin(), samples.end());

                auto begin = samples.begin();
                auto end = begin;
                double percent = m_config.percentile / 100.0;
                size_t size = percent * samples.size();
                size = std::min(size + 1, samples.size()); // To ensure that size is [1, #samples].

                std::advance(end, size);

                mxb::Duration min = *begin;
                mxb::Duration max = *end;

                mxb::Duration delta_fd = calculate_delta_fd(samples, size);
                mxb::Duration delta_sturges = calculate_delta_sturges(min, max, size);

                mxb::Duration delta = std::min(delta_fd, delta_sturges);
                int n = delta != mxb::Duration {0} ? (max - min) / delta + 1 : 1;

                DiffHistogram::Specification specification { min, delta, n };

                // Various DiffRouterSessions have a shared_ptr to the current
                // m_sHSRegistry and hence it cannot be modified. Instead we create
                // a new one. Eventually, when all canonical statements have been
                // sampled, there will be just one DiffHistogram::Specification::Registry
                // instance that everyone uses.
                auto sNext = std::make_shared<HSRegistry>(*m_sHSRegistry);
                sNext->add(canonical, specification);

                m_sHSRegistry = sNext;
                sHSRegistry = m_sHSRegistry;

                m_samples_by_canonical.erase(it);
            }
        }
    }

    return sHSRegistry;
}

std::shared_ptr<std::vector<DiffRouter::QpsEntry>> DiffRouter::lookup_qps_entries(int rwi)
{
    SQpsEntries sQps_entries;

    std::shared_lock<std::shared_mutex> shared_guard(m_rw_sQps_entries_rwlock);

    if (rwi < (int)m_rw_sQps_entries.size())
    {
        sQps_entries = m_rw_sQps_entries[rwi];
    }

    return sQps_entries;
}

std::shared_ptr<std::vector<DiffRouter::QpsEntry>> DiffRouter::get_qps_entries(int rwi)
{
    SQpsEntries sQps_entries;

    std::lock_guard<std::shared_mutex> guard(m_rw_sQps_entries_rwlock);

    if (rwi < (int)m_rw_sQps_entries.size())
    {
        sQps_entries = m_rw_sQps_entries[rwi];
    }
    else
    {
        for (int i = m_rw_sQps_entries.size(); i <= rwi; ++i)
        {
            m_rw_sQps_entries.push_back(std::make_shared<QpsEntries>());
        }

        sQps_entries = m_rw_sQps_entries[rwi];
    }

    return sQps_entries;
}

std::vector<std::shared_ptr<DiffQps>>
DiffRouter::get_qpses_for(const std::vector<const mxs::Target*>& targets)
{
    std::vector<SDiffQps> rv;

    size_t rwi = RoutingWorker::get_current()->index();

    SQpsEntries sQps_entries = lookup_qps_entries(rwi);

    if (!sQps_entries)
    {
        sQps_entries = get_qps_entries(rwi);
    }

    mxb_assert(sQps_entries);

    for (const mxs::Target* pTarget : targets)
    {
        auto end = sQps_entries->end();
        auto it = std::find_if(sQps_entries->begin(), end, [pTarget](const QpsEntry& entry) {
                return entry.pTarget == pTarget;
            });

        if (it != end)
        {
            mxb_assert(it->sQps);
            rv.push_back(it->sQps);
        }
        else
        {
            sQps_entries->emplace_back(pTarget, std::make_shared<DiffQps>(m_config.qps_window));
            rv.push_back(sQps_entries->back().sQps);
        }
    }

    return rv;
}

void DiffRouter::set_state(DiffState diff_state, SyncState sync_state)
{
    m_diff_state = diff_state;
    m_sync_state = sync_state;

#ifdef SS_DEBUG
    switch (m_diff_state)
    {
    case DiffState::PREPARED:
    case DiffState::COMPARING:
        mxb_assert(m_sync_state == SyncState::NOT_APPLICABLE);
        break;

    case DiffState::SYNCHRONIZING:
        mxb_assert(m_sync_state != SyncState::NOT_APPLICABLE);
        break;

    case DiffState::STOPPING:
        mxb_assert(m_sync_state == SyncState::SUSPENDING_SESSIONS);
    }
#endif
}

void DiffRouter::set_sync_state(SyncState sync_state)
{
    m_sync_state = sync_state;

    mxb_assert(m_diff_state == DiffState::SYNCHRONIZING
               && m_sync_state != SyncState::NOT_APPLICABLE);
}

mxs::RoutingWorker::SessionResult DiffRouter::restart_sessions()
{
    return mxs::RoutingWorker::restart_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult DiffRouter::suspend_sessions()
{
    return mxs::RoutingWorker::suspend_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult DiffRouter::resume_sessions()
{
    return mxs::RoutingWorker::resume_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult DiffRouter::suspended_sessions()
{
    return mxs::RoutingWorker::suspended_sessions(m_config.pService->name());
}

void DiffRouter::get_status(mxs::RoutingWorker::SessionResult sr, json_t** ppOutput)
{
    json_t* pOutput = json_object();
    json_object_set_new(pOutput, "state", json_string(to_string(m_diff_state)));
    json_object_set_new(pOutput, "sync_state", json_string(to_string(m_sync_state)));
    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(sr.total));
    json_object_set_new(pSessions, "suspended", json_integer(sr.affected));
    json_object_set_new(pOutput, "sessions", pSessions);

    *ppOutput = pOutput;
}

bool DiffRouter::rewire_service(const std::set<std::string>& from_targets,
                                const std::set<std::string>& to_targets)
{
    bool rv = false;

    UnmaskPasswords unmasker;

    Service* pService = static_cast<Service*>(m_config.pService);
    rv = runtime_unlink_service(pService, from_targets);

    if (rv)
    {
        rv = runtime_link_service(pService, to_targets);

        if (!rv)
        {
            MXB_ERROR("Could not link targets %s to service '%s'.",
                      mxb::join(to_targets, ",", "'").c_str(), pService->name());
        }
    }
    else
    {
        MXB_ERROR("Could not unlink targets %s from service '%s'.",
                  mxb::join(from_targets, ",", "'").c_str(), pService->name());
    }

    return rv;
}

bool DiffRouter::rewire_service_for_comparison()
{
    bool rv = false;

    std::set<std::string> from_targets { m_config.pMain->name() };
    std::set<std::string> to_targets { m_service.name() };

    rv = rewire_service(from_targets, to_targets);

    if (!rv)
    {
        MXB_ERROR("Could not rewire service '%s' for comparison.", m_config.pService->name());
    }

    return rv;
}

bool DiffRouter::rewire_service_for_normalcy()
{
    bool rv = false;

    std::set<std::string> from_targets { m_service.name() };
    std::set<std::string> to_targets { m_config.pMain->name() };

    rv = rewire_service(from_targets, to_targets);

    if (!rv)
    {
        MXB_ERROR("Could not rewire service '%s' for normalcy.", m_config.pService->name());
    }

    return rv;
}

bool DiffRouter::start_replication(const SERVER& server, ReplicationMode mode)
{
    bool rv = false;

    mxq::MariaDB mdb;

    const auto& sConfig = m_config.pService->config();

    auto& settings = mdb.connection_settings();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(server.address(), server.port()))
    {
        rv = true;

        if (mode == ReplicationMode::RESET_AND_START)
        {
            rv = mdb.cmd("RESET SLAVE");

            if (!rv)
            {
                MXB_ERROR("Could not reset replication on %s:%d, error: %s",
                          server.address(), server.port(), mdb.error());
            }
        }

        if (rv)
        {
            rv = mdb.cmd("START SLAVE");
            // TODO: It should be checked that it indeed started.

            if (!rv)
            {
                MXB_ERROR("Could not start replication on %s:%d, error: %s",
                          server.address(), server.port(), mdb.error());
            }
        }
    }
    else
    {
        MXB_ERROR("Could not open connection to %s:%d, error: %s",
                  server.address(), server.port(), mdb.error());
    }

    return rv;
}

void DiffRouter::start_replication(ReplicationMode mode)
{
    for (SERVER* pServer : m_start_replication)
    {
        if (pServer == m_config.pMain)
        {
            continue;
        }

        if (!start_replication(*pServer, mode))
        {
            MXB_ERROR("Could not %s replication of '%s'. "
                      "Manual intervention is needed.",
                      mode == ReplicationMode::RESET_AND_START ? "reset" : "start",
                      pServer->name());
        }
    }

    m_start_replication.clear();
}

void DiffRouter::start_replication()
{
    start_replication(ReplicationMode::START_ONLY);
}

void DiffRouter::reset_replication()
{
    start_replication(ReplicationMode::RESET_AND_START);
}

bool DiffRouter::stop_replication(const SERVER& server)
{
    bool rv = false;

    mxq::MariaDB mdb;

    const auto& sConfig = m_config.pService->config();

    auto& settings = mdb.connection_settings();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(server.address(), server.port()))
    {
        if (mdb.cmd("STOP ALL SLAVES"))
        {
            rv = true;
        }
        else
        {
            MXB_ERROR("Could not stop replication on %s:%d, error: %s",
                      server.address(), server.port(), mdb.error());
        }
    }
    else
    {
        MXB_ERROR("Could not open connection to %s:%d, error: %s",
                  server.address(), server.port(), mdb.error());
    }

    return rv;
}

namespace
{

using GtidPosByDomain = std::unordered_map<uint32_t, uint64_t>;

std::optional<GtidPosByDomain> get_gtid_pos_by_domain(const SERVICE& service, const SERVER& server)
{
    std::optional<GtidPosByDomain> rv;

    MariaDB mdb;

    MariaDB::ConnectionSettings& settings = mdb.connection_settings();

    const auto& sConfig = service.config();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(server.address(), server.port()))
    {
        auto sResult = mdb.query("SELECT @@gtid_current_pos");

        if (sResult)
        {
            if (sResult->next_row())
            {
                mariadb::GtidList gtids = mariadb::GtidList::from_string(sResult->get_string(0));

                GtidPosByDomain gtid_pos_by_domain;
                for (const auto& gtid : gtids.triplets())
                {
                    gtid_pos_by_domain.insert(std::make_pair(gtid.m_domain, gtid.m_sequence));
                }

                rv = gtid_pos_by_domain;
            }
        }
        else
        {
            MXB_ERROR("Could not obtain the current gtid position: %s", mdb.error());
        }
    }
    else
    {
        MXB_ERROR("Could not open connection to %s:%d: %s",
                  server.address(), server.port(), mdb.error());
    }

    return rv;
}

}

DiffRouter::ReplicationState DiffRouter::stop_replication()
{
    ReplicationState rv = ReplicationState::READY;

    SERVER* pMain = m_config.pMain;
    GtidPosByDomain from = pMain->get_gtid_list();

    auto it = m_stop_replication.begin();
    while (rv != ReplicationState::ERROR && it != m_stop_replication.end())
    {
        SERVER* pOther = *it;

        std::optional<GtidPosByDomain> to = get_gtid_pos_by_domain(m_service, *pOther);

        bool erase = false;

        if (to)
        {
            bool behind = false;

            for (auto kv : from)
            {
                auto domain = kv.first;
                auto position = kv.second;

                auto jt = to.value().find(domain);

                if (jt == to.value().end())
                {
                    MXB_DEV("Replica '%s' lacks domain %u, which is found in '%s'.",
                            pOther->name(), domain, pMain->name());
                    behind = true;
                }
                else
                {
                    if (jt->second < position)
                    {
                        MXB_DEV("The position %lu of domain %u in server '%s' is behind "
                                "the position %lu in server '%s'.",
                                jt->second, domain, pOther->name(), position, pMain->name());
                        behind = true;
                    }
                }
            }

            if (!behind)
            {
                if (stop_replication(*pOther))
                {
                    m_start_replication.push_back(pOther);
                    erase = true;
                }
                else
                {
                    rv = ReplicationState::ERROR;
                }
            }
            else
            {
                MXB_DEV("'%s' is behind '%s', not breaking replication yet.",
                        pOther->name(), pMain->name());
                rv = ReplicationState::LAGGING;
            }
        }
        else
        {
            MXB_ERROR("Could not get the Gtid positions of '%s'.",
                      pOther->name());
            rv = ReplicationState::ERROR;
        }

        if (erase)
        {
            auto distance = it - m_stop_replication.begin();
            m_stop_replication.erase(it);
            it = m_stop_replication.begin() + distance;
        }
        else
        {
            ++it;
        }
    }

    return rv;
}

void DiffRouter::restart_and_resume()
{
    RoutingWorker::SessionResult sr = restart_sessions();

    if (sr.total != sr.affected)
    {
        MXB_WARNING("Could only restart %ld out of %ld sessions of service '%s'.",
                    sr.affected, sr.total, m_config.pService->name());
    }

    sr = resume_sessions();

    if (sr.total != sr.affected)
    {
        MXB_WARNING("%ld sessions of a total of %ld of service '%s' were not suspended "
                    "when the sessions again were resumed.",
                    sr.total - sr.affected, sr.total, m_config.pService->name());
    }
}

void DiffRouter::setup(const RoutingWorker::SessionResult& sr)
{
    if (all_sessions_suspended(sr))
    {
        ReplicationState rstate = stop_replication();

        switch (rstate)
        {
        case ReplicationState::READY:
            if (rewire_service_for_comparison())
            {
                restart_and_resume();
                set_state(DiffState::COMPARING);
            }
            else
            {
                // Not sure whether rewiring actually can fail, if the arguments are ok.

                MXB_ERROR("Could not rewire '%s' service for comparison of servers. "
                          "Now attempting to reset the configuration.",
                          m_config.pService->name());

                if (rewire_service_for_normalcy())
                {
                    MXB_NOTICE("Service '%s' reset to original configuration, resuming sessions.",
                               m_config.pService->name());

                    resume_sessions();
                    set_state(DiffState::PREPARED);
                }
                else
                {
                    MXB_ERROR("Could not reset configuration of service '%s', cannot resume "
                              "sessions. This will need manual intervention.",
                              m_config.pService->name());
                }

            }
            break;

        case ReplicationState::LAGGING:
            set_sync_state(SyncState::STOPPING_REPLICATION);
            break;

        case ReplicationState::ERROR:
            MXB_ERROR("Could not stop replication, cannot rewire service '%s'. "
                      "Resuming sessions according to original configuration.",
                      m_config.pService->name());
            start_replication();
            resume_sessions();
            set_state(DiffState::PREPARED);
            break;
        }
    }
}

bool DiffRouter::setup_dcall()
{
    RoutingWorker::SessionResult sr = suspend_sessions();

    setup(sr);

    bool call_again = (m_diff_state == DiffState::SYNCHRONIZING);

    if (!call_again)
    {
        m_dcstart = 0;
    }

    return call_again;
}

void DiffRouter::start_setup_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            return setup_dcall();
        });
}

void DiffRouter::teardown(const mxs::RoutingWorker::SessionResult& sr)
{
    if (all_sessions_suspended(sr))
    {
        if (m_config.reset_replication)
        {
            reset_replication();
        }

        if (rewire_service_for_normalcy())
        {
            restart_and_resume();
        }
        else
        {
            // TODO: An ERROR state is needed also.
            mxb_assert(!true);
        }

        set_state(DiffState::PREPARED);
    }
}

bool DiffRouter::teardown_dcall()
{
    RoutingWorker::SessionResult sr = suspend_sessions();

    teardown(sr);

    bool call_again = (m_diff_state == DiffState::STOPPING);

    if (!call_again)
    {
        m_dcstart = 0;
    }

    return call_again;
}

void DiffRouter::start_teardown_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            return teardown_dcall();
        });
}

bool DiffRouter::update_exporters()
{
    bool rv = true;

    std::shared_lock<std::shared_mutex> shared_guard(m_exporters_rwlock);

    std::map<const mxs::Target*, SExporter> exporters;

    for (const mxs::Target* pTarget : m_service.get_children())
    {
        if (pTarget != m_config.pMain)
        {
            auto it = m_exporters.find(pTarget);

            if (it != m_exporters.end())
            {
                exporters.insert(*it);
            }
            else
            {
                SExporter sExporter = build_exporter(m_config, *pTarget);

                if (sExporter)
                {
                    exporters.insert(std::make_pair(pTarget, sExporter));
                }
                else
                {
                    rv = false;
                    break;
                }
            }
        }
    }

    if (rv)
    {
        shared_guard.unlock();

        std::lock_guard<std::shared_mutex> guard(m_exporters_rwlock);

        m_exporters = std::move(exporters);
    }

    return rv;
}

bool DiffRouter::collect_servers_to_be_stopped()
{
    bool rv = true;

    m_stop_replication.clear();
    m_start_replication.clear();

    std::vector<mxs::Target*> targets = m_service.get_children();
    mxb_assert(targets.size() == 2);

    SERVER* pMain = m_config.pMain;

    for (mxs::Target* pTarget : targets)
    {
        if (pTarget == pMain)
        {
            continue;
        }

        mxb_assert(pTarget->kind() == mxs::Target::Kind::SERVER);

        SERVER* pOther = static_cast<SERVER*>(pTarget);

        switch (get_replication_status(m_service, *pMain, *pOther))
        {
        case ReplicationStatus::OTHER_REPLICATES_FROM_MAIN:
            m_stop_replication.push_back(pOther);
            break;

        case ReplicationStatus::BOTH_REPLICATES_FROM_THIRD:
            break;

        case ReplicationStatus::ERROR:
        case ReplicationStatus::MAIN_REPLICATES_FROM_OTHER:
        case ReplicationStatus::NO_RELATION:
            rv = false;
            break;
        }
    }

    if (!rv)
    {
        m_stop_replication.clear();
        m_start_replication.clear();
    }

    return rv;
}
