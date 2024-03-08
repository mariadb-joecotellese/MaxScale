/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capfilter.hh"
#include "capinmemorystorage.hh"
#include "capbooststorage.hh"
#include "capconfig.hh"
#include <maxbase/stopwatch.hh>
#include <string>
#include <memory>
#include <filesystem>

namespace fs = std::filesystem;
using namespace std::string_literals;

namespace
{

const std::string DEFAULT_FILE_PREFIX = "capture";

std::string generate_file_base_name(const std::string file_prefix)
{
    auto now = wall_time::Clock::now();
    auto time_str = wall_time::to_string(now, "%F_%H%M%S");
    auto file_name = file_prefix + '_' + time_str;

    return file_name;
}
}

CapFilter::CapFilter(const std::string& name)
    : m_config(name, [this]{
    return post_configure();
})
{}

std::shared_ptr<CapRecorder> CapFilter::make_storage(const std::string file_prefix)
{
    auto base_path = m_config.capture_dir;
    base_path += '/' + generate_file_base_name(file_prefix);

    switch (m_config.storage_type)
    {
    case StorageType::BINARY:
        m_sStorage = std::make_unique<CapBoostStorage>(base_path, ReadWrite::WRITE_ONLY);
        break;
    }

    return std::make_shared<CapRecorder>(std::make_unique<RecorderContext>(m_sStorage.get()));
}


bool CapFilter::post_configure()
{
    if (m_config.start_capture)
    {
        m_sRecorder = make_storage(DEFAULT_FILE_PREFIX);
        m_sRecorder->start();
    }

    return true;
}

CapFilter::~CapFilter()
{
    if (m_sRecorder)
    {
        m_sRecorder->stop();
    }

    m_sStorage.reset();

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

    if (m_sRecorder)
    {
        sSession->start_capture(m_sRecorder);
    }

    m_sessions.push_back(sSession);

    return sSession;
}

bool CapFilter::start_capture(std::string file_prefix)
{
    stop_capture();

    std::lock_guard guard{m_sessions_mutex};

    if (file_prefix.empty())
    {
        file_prefix = DEFAULT_FILE_PREFIX;
    }

    m_sRecorder = make_storage(file_prefix);
    m_sRecorder->start();

    for (auto& w : m_sessions)
    {
        auto s = w.lock();
        if (s)
        {
            s->start_capture(m_sRecorder);
        }
    }

    return true;
}

bool CapFilter::stop_capture()
{
    std::lock_guard guard{m_sessions_mutex};

    if (m_sRecorder)
    {
        for (auto& w : m_sessions)
        {
            auto s = w.lock();
            if (s)
            {
                s->stop_capture();
            }
        }

        m_sRecorder->stop();
        m_sRecorder.reset();
        m_sStorage.reset();
    }

    return true;
}

json_t* CapFilter::diagnostics() const
{
    return m_config.to_json();
}

int64_t CapFilter::get_next_event_id() const
{
    return m_event_id.fetch_add(1, std::memory_order_relaxed);
}
