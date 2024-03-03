/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capfilter.hh"
#include "capinmemorystorage.hh"
#include "capsqlitestorage.hh"
#include "capbooststorage.hh"
#include "capconfig.hh"
#include <maxbase/stopwatch.hh>
#include <string>
#include <memory>
#include <filesystem>

namespace fs = std::filesystem;

namespace
{

std::string generate_file_base_name()
{
    auto now = wall_time::Clock::now();
    auto time_str = wall_time::to_string(now, "%F_%H%M%S");
    auto file_name = "capture_"s + time_str;

    return file_name;
}
}

CapFilter::CapFilter(const std::string& name)
    : m_config(name, [this]{
    return post_configure();
})
{}

bool CapFilter::post_configure()
{
    bool ok = true;
    auto base_path = m_config.capture_dir;
    base_path += '/' + generate_file_base_name();

    switch (m_config.storage_type)
    {
    case StorageType::SQLITE:
        m_sStorage = std::make_unique<CapSqliteStorage>(base_path);
        break;

    case StorageType::BINARY:
        m_sStorage = std::make_unique<CapBoostStorage>(base_path, ReadWrite::WRITE_ONLY);
        break;
    }

    m_sRecorder = std::make_shared<CapRecorder>(std::make_unique<RecorderContext>(m_sStorage.get()));
    m_sRecorder->start();

    return ok;
}

CapFilter::~CapFilter()
{
    m_sRecorder->stop();

    // TODO: gc_stats are useful to log. Make the stats non-global, i.e.
    //       move the counters inside GCUpdater.
    MXB_SNOTICE("Workload Capture stats:\n" << maxbase::get_collector_stats());
}

// static
CapFilter* CapFilter::create(const char* zName)
{
    return new CapFilter(zName);
}

std::shared_ptr<mxs::FilterSession> CapFilter::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    auto sSession = std::shared_ptr<CapFilterSession>(CapFilterSession::create(pSession, pService, this));

    std::lock_guard guard{m_sessions_mutex};

    sSession->start_capture(m_sRecorder);
    m_sessions.push_back(sSession);
    return sSession;
}

json_t* CapFilter::diagnostics() const
{
    return m_config.to_json();
}

int64_t CapFilter::get_next_event_id() const
{
    return m_event_id.fetch_add(1, std::memory_order_relaxed);
}
