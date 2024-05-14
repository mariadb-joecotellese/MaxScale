/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxbase/ccdefs.hh>
#include <cassert>
#include <syslog.h>
#include <stdexcept>
#include <sys/time.h>

/**
 * If MXB_MODULE_NAME is defined before log_manager.h is included, then all
 * logged messages will be prefixed with that string enclosed in square brackets.
 * For instance, the following
 *
 *     #define MXB_MODULE_NAME "xyz"
 *     #include <log_manager.h>
 *
 * will lead to every logged message looking like:
 *
 *     2016-08-12 13:49:11   error : [xyz] The gadget was not ready
 *
 * In general, the value of MXB_MODULE_NAME should be the name of the shared
 * library to which the source file, where MXB_MODULE_NAME is defined, belongs.
 *
 * Note that a file that is compiled into multiple modules should
 * have MXB_MODULE_NAME defined as something else than the name of a real
 * module, or not at all.
 *
 * Any file that is compiled into maxscale-common should *not* have
 * MXB_MODULE_NAME defined.
 */
#if !defined (MXB_MODULE_NAME)
#define MXB_MODULE_NAME NULL
#endif

extern int mxb_log_enabled_priorities;

typedef enum mxb_log_augmentation_t
{
    MXB_LOG_AUGMENT_WITH_FUNCTION = 1,      // Each logged line is suffixed with [function-name]
    MXB_LOG_AUGMENTATION_MASK     = (MXB_LOG_AUGMENT_WITH_FUNCTION)
} mxb_log_augmentation_t;

typedef struct MXB_LOG_THROTTLING
{
    size_t count;       // Maximum number of a specific message...
    size_t window_ms;   // ...during this many milliseconds.
    size_t suppress_ms; // If exceeded, suppress such messages for this many ms.
} MXB_LOG_THROTTLING;

/**
 * @brief Has the log been initialized.
 *
 * @return True if the log has been initialized, false otherwise.
 */
bool mxb_log_inited();
/**
 * Rotate the log
 *
 * @return True if the rotating was successful
 */
bool mxb_log_rotate();

/**
 * Get log filename
 *
 * @return The current filename.
 *
 * @attention This function can be called only after @c mxb_log_init() has
 *            been called and @c mxb_log_finish() has not been called. The
 *            returned filename stays valid only until @c mxb_log_finish()
 *            is called.
 */
const char* mxb_log_get_filename();

/**
 * Enable/disable a particular syslog priority.
 *
 * @param priority  One of the LOG_ERR etc. constants from sys/syslog.h.
 * @param enabled   True if the priority should be enabled, false if it should be disabled.
 *
 * @return True if the priority was valid, false otherwise.
 */
bool mxb_log_set_priority_enabled(int priority, bool enabled);

bool mxb_log_get_session_trace();

/**
 * Convert log level to string
 */
const char* mxb_log_level_to_string(int level);

/**
 * Query whether a particular syslog priority is enabled.
 *
 * Don't use this to check whether a message at a particular level should be logged,
 * use @c mxb_log_should_log instead.
 *
 * @param priority  One of the LOG_ERR etc. constants from sys/syslog.h.
 *
 * @return True if enabled, false otherwise.
 */
static inline bool mxb_log_is_priority_enabled(int priority)
{
    assert((priority & ~LOG_PRIMASK) == 0);
    return ((mxb_log_enabled_priorities & (1 << priority)) != 0) || (priority == LOG_ALERT);
}

/**
 * Enable/disable syslog logging.
 *
 * @param enabled True, if syslog logging should be enabled, false if it should be disabled.
 */
void mxb_log_set_syslog_enabled(bool enabled);

/**
 * Is syslog logging enabled.
 *
 * @return True if enabled, false otherwise.
 */
bool mxb_log_is_syslog_enabled();

/**
 * Enable/disable maxscale log logging.
 *
 * @param enabled True, if maxlog logging should be enabled, false if it should be disabled.
 */
void mxb_log_set_maxlog_enabled(bool enabled);

/**
 * Is maxlog logging enabled.
 *
 * @return True if enabled, false otherwise.
 */
bool mxb_log_is_maxlog_enabled();

/**
 * Enable/disable highprecision logging.
 *
 * @param enabled True, if high precision logging should be enabled, false if it should be disabled.
 */
void mxb_log_set_highprecision_enabled(bool enabled);

/**
 * Is highprecision logging enabled.
 *
 * @return True if enabled, false otherwise.
 */
bool mxb_log_is_highprecision_enabled();

/**
 * Set the augmentation
 *
 * @param bits  Combination of @c mxb_log_augmentation_t values.
 */
void mxb_log_set_augmentation(int bits);

/**
 * Set the log throttling parameters.
 *
 * @param throttling The throttling parameters.
 */
void mxb_log_set_throttling(const MXB_LOG_THROTTLING* throttling);

/**
 * Get the log throttling parameters.
 *
 * @param throttling The throttling parameters.
 */
void mxb_log_get_throttling(MXB_LOG_THROTTLING* throttling);

/**
 * Resets the suppression of messages done by log throttling
 */
void mxb_log_reset_suppression();

/**
 * Redirect  stdout to the log file
 *
 * @param redirect Whether to redirect the output to the log file
 */
void mxb_log_redirect_stdout(bool redirect);

/**
 * Set session specific in-memory log
 *
 * @param enabled True or false to enable or disable session in-memory logging
 */
void mxb_log_set_session_trace(bool enabled);


/**
 * Log a message of a particular priority.
 *
 * @param priority One of the syslog constants: LOG_ERR, LOG_WARNING, ...
 * @param modname  The name of the module.
 * @param file     The name of the file where the message was logged.
 * @param line     The line where the message was logged.
 * @param function The function where the message was logged.
 * @param format   The printf format of the following arguments.
 * @param ...      Optional arguments according to the format.
 *
 * @return 0 for success, non-zero otherwise.
 */
int mxb_log_message(int priority,
                    const char* modname,
                    const char* file,
                    int line,
                    const char* function,
                    const char* format,
                    ...) mxb_attribute((format(printf, 6, 7)));

/**
 * Log a fatal error message.
 *
 * @param message  The message to be logged.
 *
 * @attention The literal string should have a trailing "\n".
 *
 * @return 0 for success, non-zero otherwise.
 */
int mxb_log_fatal_error(const char* message);

/**
 * Check if a message at this priority should be logged in the current context
 *
 * This function takes the current context (i.e. the session) into consideration when
 * inspecting whether the message should be logged.
 *
 * @param priority The log priority of the message
 *
 * @return True if the message should be logged
 */
bool mxb_log_should_log(int priority);

/**
 * Log an error, warning, notice, info, or debug  message.
 *
 * @param priority One of the syslog constants (LOG_ERR, LOG_WARNING, ...)
 * @param format   The printf format of the message.
 * @param ...      Arguments, depending on the format.
 *
 * @return 0 for success, non-zero otherwise.
 *
 * @attention Should typically not be called directly. Use some of the
 *            MXB_ERROR, MXB_WARNING, etc. macros instead.
 */
#define MXB_LOG_MESSAGE(priority, format, ...) \
    (mxb_log_should_log(priority)  \
     ? mxb_log_message(priority, MXB_MODULE_NAME, __FILE__, __LINE__, __func__, format, ##__VA_ARGS__)  \
     : 0)

/**
 * Log an alert, error, warning, notice, info, or debug  message.
 *
 * MXB_ALERT   Not throttled  To be used when the system is about to go down in flames.
 * MXB_ERROR   Throttled      For errors.
 * MXB_WARNING Throttled      For warnings.
 * MXB_NOTICE  Not Throttled  For messages deemed important, typically used during startup.
 * MXB_INFO    Not Throttled  For information thought to be of value for investigating some problem.
 * MXB_DEBUG   Not Throttled  For debugging messages, enabled in debug builds. Should not be added
 *                            willy-nilly so as not to make it hard to see the forest for the trees.
 * MXB_DEV     Not Throttled  For development time messages, logged as notices, enabled in debug builds.
 *                            Must be removed when the development is ready.
 *
 * @param format The printf format of the message.
 * @param ...    Arguments, depending on the format.
 *
 * @return 0 for success, non-zero otherwise.
 */
#define MXB_ALERT(format, ...)   MXB_LOG_MESSAGE(LOG_ALERT, format, ##__VA_ARGS__)
#define MXB_ERROR(format, ...)   MXB_LOG_MESSAGE(LOG_ERR, format, ##__VA_ARGS__)
#define MXB_WARNING(format, ...) MXB_LOG_MESSAGE(LOG_WARNING, format, ##__VA_ARGS__)
#define MXB_NOTICE(format, ...)  MXB_LOG_MESSAGE(LOG_NOTICE, format, ##__VA_ARGS__)
#define MXB_INFO(format, ...)    MXB_LOG_MESSAGE(LOG_INFO, format, ##__VA_ARGS__)

#if defined (SS_DEBUG)
#define MXB_DEBUG(format, ...) MXB_LOG_MESSAGE(LOG_DEBUG, format, ##__VA_ARGS__)
#define MXB_DEV(format, ...)   MXB_LOG_MESSAGE(LOG_NOTICE, format, ##__VA_ARGS__)
#else
#define MXB_DEBUG(format, ...)
#define MXB_DEV(format, ...)
#endif

#define MXB_STREAM_LOG_HELPER(CMXBLOGLEVEL__, mxb_msg_str__) \
    do { \
        if (!mxb_log_is_priority_enabled(CMXBLOGLEVEL__)) \
        { \
            break; \
        } \
        std::ostringstream os; \
        os << mxb_msg_str__; \
        mxb_log_message(CMXBLOGLEVEL__, MXB_MODULE_NAME, __FILE__, __LINE__, \
                        __func__, "%s", os.str().c_str()); \
    } while (false)


/**
 * Same as the ones without the 'S' above, but applying the argument to
 * a std::ostringstream. Example usage: MXB_SDEV("a = " << a);
 */
#define MXB_SALERT(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_ALERT, mxb_msg_str__)
#define MXB_SERROR(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_ERR, mxb_msg_str__)
#define MXB_SWARNING(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_WARNING, mxb_msg_str__)
#define MXB_SNOTICE(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_NOTICE, mxb_msg_str__)
#define MXB_SINFO(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_INFO, mxb_msg_str__)

#if defined (SS_DEBUG)
#define MXB_SDEBUG(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_DEBUG, mxb_msg_str__)
#else
#define MXB_SDEBUG(mxb_msg_str__)
#endif

#if defined (SS_DEBUG)
#define MXB_SDEV(mxb_msg_str__) MXB_STREAM_LOG_HELPER(LOG_NOTICE, mxb_msg_str__)
#else
#define MXB_SDEV(mxb_msg_str__)
#endif

/**
 * Log an out of memory error using custom message.
 *
 * @param message  Text to be logged. Must be a literal string.
 *
 * @return 0 for success, non-zero otherwise.
 */
#define MXB_OOM_MESSAGE(message) mxb_log_fatal_error("OOM: " message "\n")

#define MXB_OOM_FROM_STRINGIZED_MACRO(macro) MXB_OOM_MESSAGE(#macro)

/**
 * Log an out of memory error using a default message.
 *
 * @return 0 for success, non-zero otherwise.
 */
#define MXB_OOM() MXB_OOM_FROM_STRINGIZED_MACRO(__func__)

/**
 * Log an out of memory error using a default message, if the
 * provided pointer is NULL.
 *
 * @param p  If NULL, an OOM message will be logged.
 *
 * @return 0 for success, non-zero otherwise.
 */
#define MXB_OOM_IFNULL(p) do {if (!p) {MXB_OOM();}} while (false)

/**
 * Log an out of memory error using custom message, if the
 * provided pointer is NULL.
 *
 * @param p        If NULL, an OOM message will be logged.
 * @param message  Text to be logged. Must be literal string.
 *
 * @return 0 for success, non-zero otherwise.
 */
#define MXB_OOM_MESSAGE_IFNULL(p, message) do {if (!p) {MXB_OOM_MESSAGE(message);}} while (false)


inline bool operator==(const MXB_LOG_THROTTLING& lhs, const MXB_LOG_THROTTLING& rhs)
{
    return lhs.count == rhs.count && lhs.window_ms == rhs.window_ms && lhs.suppress_ms == rhs.suppress_ms;
}

enum mxb_log_target_t
{
    MXB_LOG_TARGET_DEFAULT,
    MXB_LOG_TARGET_FS,      // File system
    MXB_LOG_TARGET_STDOUT,  // Standard output
    MXB_LOG_TARGET_STDERR,  // Standard error
};

/**
 * Prototype for function providing additional information.
 *
 * If the function returns a non-zero value, that amount of characters
 * will be enclosed between '(' and ')', and written first to a logged
 * message.
 *
 * @param buffer  Buffer where additional context may be written.
 * @param len     Length of @c buffer.
 *
 * @return Length of data written to buffer.
 */
using mxb_log_context_provider_t = size_t (*)(char* buffer, size_t len);

/**
 * Prototype for function to be called when session tracing.
 *
 * @param timestamp The timestamp for the message.
 * @param message   The message abount to be logged.
 */
using mxb_in_memory_log_t = void (*)(struct timeval timestamp, std::string_view message);

/**
 * Prototype for conditional logging callback.
 *
 * @param priority The syslog priority under which the message is logged.
 *
 * @return True if the message should be logged, false if it should be suppressed.
 */
using mxb_should_log_t = bool (*)(int priority);

/**
 * @brief Initialize the log
 *
 * This function must be called before any of the log function should be
 * used.
 *
 * @param ident             The syslog ident. If @c nullptr, then the program name is used.
 * @param logdir            The directory for the log file. If @c nullptr, file output is discarded.
 * @param filename          The name of the log-file. If @c nullptr, the program name will be used
 *                          if it can be deduced, otherwise the name will be "messages.log".
 * @param target            Logging target
 * @param context_provider  Optional function for providing contextual information
 *                          at logging time.
 * @param in_memory_log     Optional function that will be called if session tracing will be
 *                          enabled.
 * @param should_log        Optional function that will be called when deciding whether to
 *                          actually log a message or not.
 *
 * @return true if succeed, otherwise false
 */
bool mxb_log_init(const char* ident,
                  const char* logdir,
                  const char* filename,
                  mxb_log_target_t target,
                  mxb_log_context_provider_t context_provider,
                  mxb_in_memory_log_t in_memory_log,
                  mxb_should_log_t should_log);

/**
 * @brief Finalize the log
 *
 * A successfull call to @c max_log_init() should be followed by a call
 * to this function before the process exits.
 */
void mxb_log_finish();

/**
 * @brief Initialize the log
 *
 * This function initializes the log using
 * - the program name as the syslog ident,
 * - the current directory as the logdir, and
 * - the default log name (program name + ".log").
 *
 * @param target  The specified target for the logging.
 *
 * @return True if succeeded, false otherwise.
 */
inline bool mxb_log_init(mxb_log_target_t target = MXB_LOG_TARGET_FS)
{
    const char* log_dir = target == MXB_LOG_TARGET_FS ? "." : nullptr;

    return mxb_log_init(nullptr, log_dir, nullptr, target, nullptr, nullptr, nullptr);
}

namespace maxbase
{

/**
 * Convert the given time value to a log timestamp
 *
 * @param tv            The time to convert to a log timestamp string
 * @param highprecision If true, the timestamp will contain milliseconds
 *
 * @return The log timestamp string
 */
std::string format_timestamp(const struct timeval& tv, bool highprecision);

/**
 * @class Log
 *
 * A simple utility RAII class where the constructor initializes the log and
 * the destructor finalizes it.
 */
class Log
{
    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;

public:
    Log(const char* ident,
        const char* logdir,
        const char* filename,
        mxb_log_target_t target,
        mxb_log_context_provider_t context_provider,
        mxb_in_memory_log_t in_memory_log,
        mxb_should_log_t should_log)
    {
        if (!mxb_log_init(ident, logdir, filename, target, context_provider, in_memory_log, should_log))
        {
            throw std::runtime_error("Failed to initialize the log.");
        }
    }

    Log(mxb_log_target_t target = MXB_LOG_TARGET_FS)
        : Log(nullptr, ".", nullptr, target, nullptr, nullptr, nullptr)
    {
    }

    ~Log()
    {
        mxb_log_finish();
    }
};

// RAII class for setting and clearing the "scope" of the log messages. Adds the given object name to log
// messages as long as the object is alive.
class LogScope
{
public:
    LogScope(const LogScope&) = delete;
    LogScope& operator=(const LogScope&) = delete;

    explicit LogScope(const char* name)
        : m_prev_scope(s_current_scope)
        , m_name(name)
    {
        s_current_scope = this;
    }

    ~LogScope()
    {
        s_current_scope = m_prev_scope;
    }

    static const char* current_scope()
    {
        return s_current_scope ? s_current_scope->m_name : nullptr;
    }

private:
    LogScope*   m_prev_scope;
    const char* m_name;

    static thread_local LogScope* s_current_scope;
};

// Class for redirecting the thread-local log message stream to a different handler. Only one of these should
// be constructed in the callstack.
class LogRedirect
{
public:
    LogRedirect(const LogRedirect&) = delete;
    LogRedirect& operator=(const LogRedirect&) = delete;

    /**
     * The message handler type
     *
     * @param level Syslog log level of the message
     * @param msg   The message itself
     *
     * @return True if the message was consumed (i.e. it should not be logged)
     */
    using Func = bool (*)(int level, std::string_view msg);

    explicit LogRedirect(Func func);
    ~LogRedirect();

    static Func current_redirect();

private:
    static thread_local Func s_redirect;
};
}
