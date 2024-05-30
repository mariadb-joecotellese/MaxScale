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
#include <maxbase/pretty_print.hh>
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

    return std::make_shared<CapRecorder>(std::make_unique<RecorderContext>(
        std::make_unique<CapBoostStorage>(base_path, ReadWrite::WRITE_ONLY)
        ));
}

void CapFilter::start_recording(std::shared_ptr<CapRecorder> sRecorder)
{
    // This function must be called with m_sessions_mutex locked.
    SimTime::reset_sim_time(wall_time::Clock::now());
    m_capture_stop_triggered = false;
    m_start_time = mxb::Clock::now(mxb::NowType::EPollTick);
    m_sRecorder = std::move(sRecorder);
    m_sRecorder->start();
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
        try
        {
            auto sRecorder = make_storage(DEFAULT_FILE_PREFIX);

            std::lock_guard guard{m_sessions_mutex};
            start_recording(std::move(sRecorder));
        }
        catch (const std::exception& ex)
        {
            MXB_ERROR("Failed to open storage: %s", ex.what());
            return false;
        }
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

    // TODO: gc_stats are useful to log. Make the stats non-global, i.e.
    //       move the counters inside GCUpdater.
    MXB_SNOTICE("Workload Capture stats:\n" << maxbase::get_collector_stats());
}

std::string CapFilter::parse_cmd_line_options(const std::string options)
{
    auto key_values = parse_key_value_pairs(options);
    if (key_values.empty() && !options.empty())
    {
        MXB_THROW(WcarError, "invalid options to start command: '" << options << '\'');
    }

    auto file_prefix = DEFAULT_FILE_PREFIX;
    std::chrono::milliseconds new_duration{0};
    uint64_t new_size = 0;

    for (auto& [key, value] : key_values)
    {
        if (key == "prefix")
        {
            file_prefix = value;
        }
        else if (key == "duration")
        {
            if (!get_suffixed_duration(value.c_str(), &new_duration))
            {
                MXB_THROW(WcarError, "invalid duration option: '" << value << '\'');
            }
        }
        else if (key == "size")
        {
            if (!get_suffixed_size(value.c_str(), &new_size))
            {
                MXB_THROW(WcarError, "invalid size option: '" << value << '\'');
            }
        }
        else
        {
            MXB_THROW(WcarError, "invalid option key: '" << key << '\'');
        }
    }

    m_capture_duration = new_duration.count() ? new_duration : m_config.capture_duration;
    m_capture_size = new_size ? new_size : m_config.capture_size;

    return file_prefix;
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

bool CapFilter::start_capture(const std::string& options)
{
    stop_capture();

    auto file_prefix = parse_cmd_line_options(options);

    // The call to make_storage() will end up calling RoutingWorker::call() which must not be done while
    // holding m_session_mutex as the same lock is acquired in newSession() that's executed by the
    // RoutingWorkers.
    auto sRecorder = make_storage(file_prefix);

    std::lock_guard guard{m_sessions_mutex};
    start_recording(std::move(sRecorder));

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
