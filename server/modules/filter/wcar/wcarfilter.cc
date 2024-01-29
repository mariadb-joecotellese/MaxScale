/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarfilter.hh"
#include "wcarinmemorystorage.hh"
#include "wcarsqlitestorage.hh"
#include "wcarbooststorage.hh"
#include "wcarconfig.hh"
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

WcarFilter::WcarFilter(const std::string& name)
    : m_config(name, [this]{
    return post_configure();
})
{}

bool WcarFilter::post_configure()
{
    bool ok = true;
    auto base_path = m_config.capture_dir;
    base_path += '/' + generate_file_base_name();

    switch (m_config.storage_type)
    {
    case StorageType::SQLITE:
        m_sStorage = std::make_unique<SqliteStorage>(base_path);
        break;

    case StorageType::BINARY:
        m_sStorage = std::make_unique<BoostStorage>(base_path);
        break;
    }

    m_sRecorder = std::make_unique<WcarRecorder>(std::make_unique<RecorderContext>(m_sStorage.get()));
    m_sRecorder->start();

    return ok;
}

WcarFilter::~WcarFilter()
{
    m_sRecorder->stop();

    auto s = maxbase::get_collector_stats();
    // TODO: gc_stats are useful to log. Make the stats non-global, i.e.
    //       move the counters inside GCUpdater.
    MXB_SNOTICE("Workload Capture stats:\n" << maxbase::get_collector_stats());
}

// static
WcarFilter* WcarFilter::create(const char* zName)
{
    return new WcarFilter(zName);
}

WcarFilterSession* WcarFilter::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    return WcarFilterSession::create(pSession, pService, this);
}

json_t* WcarFilter::diagnostics() const
{
    return m_config.to_json();
}

int64_t WcarFilter::get_next_event_id() const
{
    return m_event_id.fetch_add(1, std::memory_order_relaxed);
}
