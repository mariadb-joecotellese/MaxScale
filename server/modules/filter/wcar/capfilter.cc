/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capfilter.hh"
#include "capbooststorage.hh"
#include "capconfig.hh"
#include "simtime.hh"
#include <maxscale/mainworker.hh>
#include <maxbase/stopwatch.hh>
#include <string>
#include <memory>
#include <filesystem>

namespace fs = std::filesystem;
using namespace std::string_literals;
using namespace std::chrono_literals;

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
    : mxb::Worker::Callable(mxs::MainWorker::get())
    , m_config(name, [this]{
    return post_configure();
})
{
    SimTime::reset_sim_time(wall_time::Clock::now());
}

std::shared_ptr<CapRecorder> CapFilter::make_storage(const std::string file_prefix)
{
    auto base_path = m_config.capture_directory();
    base_path += '/' + generate_file_base_name(file_prefix);

    m_sStorage = std::make_unique<CapBoostStorage>(base_path, ReadWrite::WRITE_ONLY);

    m_capture_stop_triggered = false;
    m_start_time = mxb::Clock::now(mxb::NowType::EPollTick);

    return std::make_shared<CapRecorder>(std::make_unique<RecorderContext>(m_sStorage.get()));
}

bool CapFilter::supervise()
{
    if (m_sRecorder && !m_capture_stop_triggered)
    {
        auto sz_limit = m_capture_size != 0
            && m_sRecorder->context().bytes_processed() >= m_capture_size;

        m_capture_stop_triggered = sz_limit || (m_capture_duration.count() != 0
            && mxb::Clock::now(mxb::NowType::EPollTick) - m_start_time > m_capture_duration);

        if (m_capture_stop_triggered)
        {
            stop_capture();
        }
    }

    return true;
}

bool CapFilter::post_configure()
{
    if (m_config.start_capture)
    {
        SimTime::reset_sim_time(wall_time::Clock::now());
        m_sRecorder = make_storage(DEFAULT_FILE_PREFIX);
        m_sRecorder->start();
    }

    m_dc_supervisor = dcall(1s, [this]() {
        return supervise();
    });

    m_capture_duration = m_config.capture_duration;
    m_capture_size = m_config.capture_size;

    return true;
}

CapFilter::~CapFilter()
{
    if (m_dc_supervisor != mxb::Worker::NO_CALL)
    {
        cancel_dcall(m_dc_supervisor);
    }

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

    SimTime::reset_sim_time(wall_time::Clock::now());
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
    mxb::Json js;

    if (m_sRecorder)
    {
        js.set_bool("capturing", true);
        js.set_real("duration", mxb::to_secs(mxb::Clock::now() - m_start_time));
        js.set_int("size", m_sRecorder->context().bytes_processed());
    }
    else
    {
        js.set_bool("capturing", false);
    }

    return js.release();
}

int64_t CapFilter::get_next_event_id() const
{
    return m_event_id.fetch_add(1, std::memory_order_relaxed);
}
