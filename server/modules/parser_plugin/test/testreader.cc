/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "testreader.hh"
#include <algorithm>
#include <map>
#include <iostream>
#include <maxbase/string.hh>

using std::istream;
using std::string;
using std::map;

namespace
{

enum skip_action_t
{
    SKIP_NOTHING,       // Skip nothing.
    SKIP_BLOCK,         // Skip until the end of next { ... }
    SKIP_DELIMITER,     // Skip the new delimiter.
    SKIP_LINE,          // Skip current line.
    SKIP_NEXT_STATEMENT,// Skip statement starting on line following this line.
    SKIP_STATEMENT,     // Skip statment starting on this line.
    SKIP_TERMINATE,     // Cannot handle this, terminate.
};

typedef std::map<std::string, skip_action_t> KeywordActionMapping;

static KeywordActionMapping mtl_keywords;
static KeywordActionMapping plsql_keywords;

void init_keywords()
{
    struct Keyword
    {
        const char*   z_keyword;
        skip_action_t action;
    };

    static const Keyword MTL_KEYWORDS[] =
    {
        {"append_file",                SKIP_LINE          },
        {"cat_file",                   SKIP_LINE          },
        {"change_user",                SKIP_LINE          },
        {"character_set",              SKIP_LINE          },
        {"chmod",                      SKIP_LINE          },
        {"connect",                    SKIP_LINE          },
        {"connection",                 SKIP_LINE          },
        {"copy_file",                  SKIP_LINE          },
        {"dec",                        SKIP_LINE          },
        {"delimiter",                  SKIP_DELIMITER     },
        {"die",                        SKIP_LINE          },
        {"diff_files",                 SKIP_LINE          },
        {"dirty_close",                SKIP_LINE          },
        {"disable_abort_on_error",     SKIP_LINE          },
        {"disable_connect_log",        SKIP_LINE          },
        {"disable_info",               SKIP_LINE          },
        {"disable_metadata",           SKIP_LINE          },
        {"disable_parsing",            SKIP_LINE          },
        {"disable_ps_protocol",        SKIP_LINE          },
        {"disable_query_log",          SKIP_LINE          },
        {"disable_reconnect",          SKIP_LINE          },
        {"disable_result_log",         SKIP_LINE          },
        {"disable_rpl_parse",          SKIP_LINE          },
        {"disable_session_track_info", SKIP_LINE          },
        {"disable_warnings",           SKIP_LINE          },
        {"disconnect",                 SKIP_LINE          },
        {"echo",                       SKIP_LINE          },
        {"enable_abort_on_error",      SKIP_LINE          },
        {"enable_connect_log",         SKIP_LINE          },
        {"enable_info",                SKIP_LINE          },
        {"enable_metadata",            SKIP_LINE          },
        {"enable_parsing",             SKIP_LINE          },
        {"enable_ps_protocol",         SKIP_LINE          },
        {"enable_query_log",           SKIP_LINE          },
        {"enable_reconnect",           SKIP_LINE          },
        {"enable_result_log",          SKIP_LINE          },
        {"enable_rpl_parse",           SKIP_LINE          },
        {"enable_session_track_info",  SKIP_LINE          },
        {"enable_warnings",            SKIP_LINE          },
        {"end_timer",                  SKIP_LINE          },
        {"error",                      SKIP_NEXT_STATEMENT},
        {"eval",                       SKIP_STATEMENT     },
        {"exec",                       SKIP_LINE          },
        {"file_exists",                SKIP_LINE          },
        {"horizontal_results",         SKIP_LINE          },
        {"inc",                        SKIP_LINE          },
        {"let",                        SKIP_LINE          },
        {"let",                        SKIP_LINE          },
        {"list_files",                 SKIP_LINE          },
        {"list_files_append_file",     SKIP_LINE          },
        {"list_files_write_file",      SKIP_LINE          },
        {"lowercase_result",           SKIP_LINE          },
        {"mkdir",                      SKIP_LINE          },
        {"move_file",                  SKIP_LINE          },
        {"output",                     SKIP_LINE          },
        {"perl",                       SKIP_TERMINATE     },
        {"ping",                       SKIP_LINE          },
        {"print",                      SKIP_LINE          },
        {"query",                      SKIP_LINE          },
        {"query_get_value",            SKIP_LINE          },
        {"query_horizontal",           SKIP_LINE          },
        {"query_vertical",             SKIP_LINE          },
        {"real_sleep",                 SKIP_LINE          },
        {"reap",                       SKIP_LINE          },
        {"remove_file",                SKIP_LINE          },
        {"remove_files_wildcard",      SKIP_LINE          },
        {"replace_column",             SKIP_LINE          },
        {"replace_regex",              SKIP_LINE          },
        {"replace_result",             SKIP_LINE          },
        {"require",                    SKIP_LINE          },
        {"reset_connection",           SKIP_LINE          },
        {"result",                     SKIP_LINE          },
        {"result_format",              SKIP_LINE          },
        {"rmdir",                      SKIP_LINE          },
        {"same_master_pos",            SKIP_LINE          },
        {"send",                       SKIP_LINE          },
        {"send_eval",                  SKIP_LINE          },
        {"send_quit",                  SKIP_LINE          },
        {"send_shutdown",              SKIP_LINE          },
        {"skip",                       SKIP_LINE          },
        {"sleep",                      SKIP_LINE          },
        {"sorted_result",              SKIP_LINE          },
        {"source",                     SKIP_LINE          },
        {"start_timer",                SKIP_LINE          },
        {"sync_slave_with_master",     SKIP_LINE          },
        {"sync_with_master",           SKIP_LINE          },
        {"system",                     SKIP_LINE          },
        {"vertical_results",           SKIP_LINE          },
        {"write_file",                 SKIP_LINE          },
    };

    const size_t N_MTL_KEYWORDS = sizeof(MTL_KEYWORDS) / sizeof(MTL_KEYWORDS[0]);

    for (size_t i = 0; i < N_MTL_KEYWORDS; ++i)
    {
        mtl_keywords[MTL_KEYWORDS[i].z_keyword] = MTL_KEYWORDS[i].action;
    }

    static const Keyword PLSQL_KEYWORDS[] =
    {
        {"exit",  SKIP_LINE },
        {"if",    SKIP_BLOCK},
        {"while", SKIP_BLOCK},
    };

    const size_t N_PLSQL_KEYWORDS = sizeof(PLSQL_KEYWORDS) / sizeof(PLSQL_KEYWORDS[0]);

    for (size_t i = 0; i < N_PLSQL_KEYWORDS; ++i)
    {
        plsql_keywords[PLSQL_KEYWORDS[i].z_keyword] = PLSQL_KEYWORDS[i].action;
    }
}

skip_action_t get_action(const string& keyword, const string& delimiter)
{
    skip_action_t action = SKIP_NOTHING;

    string key(keyword);
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    if (key == "delimiter")
    {
        // DELIMITER is directly understood by the parser so it needs to
        // be handled explicitly.
        action = SKIP_DELIMITER;
    }
    else
    {
        KeywordActionMapping::iterator i = mtl_keywords.find(key);

        if (i != mtl_keywords.end())
        {
            action = i->second;
        }
    }

    if ((action == SKIP_NOTHING) && (delimiter == ";"))
    {
        // Some mysqltest keywords, such as "while", "exit" and "if" are also
        // PL/SQL keywords. We assume they can only be used in the former role,
        // if the delimiter is ";".
        KeywordActionMapping::iterator i = plsql_keywords.find(key);

        if (i != plsql_keywords.end())
        {
            action = i->second;
        }
    }

    return action;
}
}

namespace maxscale
{

TestReader::TestReader(istream& in,
                       size_t line)
    : m_expect(Expect::MARIADB)
    , m_in(in)
    , m_line(line)
    , m_delimiter(";")
{
    init();
}

TestReader::TestReader(Expect expect,
                       istream& in,
                       size_t line)
    : m_expect(expect)
    , m_in(in)
    , m_line(line)
    , m_delimiter(";")
{
    init();
}

TestReader::result_t TestReader::get_statement(std::string& stmt)
{
    bool error;     // Whether an error has occurred.
    bool found;     // Whether we have found a statement.
    bool skip;      // Whether next statement should be skipped.
    bool postgres_skip;

    stmt.clear();

    string line;

    do
    {
        error = false;
        found = false;
        skip = false;
        postgres_skip = false;

        while (!error && !found && std::getline(m_in, line))
        {
            m_line++;

            mxb::trim(line);

            if (line.empty())
            {
                continue;
            }

            if (is_postgres())
            {
                // Ignore all meta-commands such as '\d'.
                if (line.substr(0, 1) == "\\")
                {
                    if (!stmt.empty())
                    {
                        // If something has been read already, then this will also
                        // terminate the statement.
                        found = true;
                    }
                    continue;
                }

                string l = line;
                mxb::lower_case(l);

                if (l.find("-- error") != string::npos
                    || l.find("-- fail") != string::npos)
                {
                    postgres_skip = true;
                }

                // If '-- fail' or '-- bogus', then the following statements are expected to fail.
                if (l.substr(0, 7) == "-- fail" || l.substr(0, 8) == "-- bogus")
                {
                    skip_postgres_until_ok();
                    continue;
                }

                // In Postgres can only be a comment and not a mysqltest command.
                if (line.substr(0, 2) == "--")
                {
                    continue;
                }

                if (strncasecmp(line.c_str(), "copy", 4) == 0)
                {
                    // Apparently a COPY statement. Does it read from stdin?
                    if (strcasestr(line.c_str(), "stdin") != nullptr)
                    {
                        skip_postgres_stdin_input();
                        continue;
                    }
                }

                auto i = line.find("/*");

                if (i != string::npos)
                {
                    auto j = line.find("*/", i + 2);

                    if (j != string::npos)
                    {
                        // Single line /* ... */ comment. Assume there is just one and remove it.
                        line = line.substr(0, i) + line.substr(j + 2);
                    }
                    else
                    {
                        line = line.substr(0, i);

                        string ln;
                        skip_postgres_block_quote(ln);

                        if (!ln.empty())
                        {
                            line += " ";
                            line += ln;
                        }
                    }

                    mxb::trim(line);
                    if (line.empty())
                    {
                        continue;
                    }
                }
            }

            auto i = line.find("-- ");

            if (i != string::npos && i != 0)
            {
                // A "-- " not the the beginning, so has to be a regular comment.
                line = line.substr(0, i);
                mxb::rtrim(line);
            }

            if (line.at(0) != '#')
            {
                // Ignore comment lines.
                if ((line.substr(0, 3) == "-- ") || (line.substr(0, 1) == "#"))
                {
                    continue;
                }

                if (!skip)
                {
                    if (line.substr(0, 2) == "--")
                    {
                        line = line.substr(2);
                        mxb::trim(line);
                    }

                    string::iterator it = std::find_if(line.begin(), line.end(), ::isspace);
                    string keyword = line.substr(0, it - line.begin());

                    skip_action_t action = get_action(keyword, m_delimiter);

                    switch (action)
                    {
                    case SKIP_NOTHING:
                        break;

                    case SKIP_BLOCK:
                        skip_block();
                        continue;

                    case SKIP_DELIMITER:
                        line = line.substr(it - line.begin());
                        mxb::trim(line);
                        if (line.length() > 0)
                        {
                            if (line.length() >= m_delimiter.length())
                            {
                                if (line.substr(line.length() - m_delimiter.length()) == m_delimiter)
                                {
                                    m_delimiter = line.substr(0, line.length() - m_delimiter.length());
                                }
                                else
                                {
                                    m_delimiter = line;
                                }
                            }
                            else
                            {
                                m_delimiter = line;
                            }
                        }
                        continue;

                    case SKIP_LINE:
                        continue;

                    case SKIP_NEXT_STATEMENT:
                        skip = true;
                        continue;

                    case SKIP_STATEMENT:
                        skip = true;
                        break;

                    case SKIP_TERMINATE:
                        MXB_ERROR("Cannot handle line %u: %s", (unsigned)m_line, line.c_str());
                        error = true;
                        break;
                    }
                }

                // If there is a trailing comment "CREATE TABLE t ( -- t is for ...", remove
                // it before appending to the statement.
                i = line.find("-- ");
                if (i != string::npos)
                {
                    line = line.substr(0, i);
                }

                stmt += line;

                if (is_postgres())
                {
                    skip_postgres_dollar_quotes(line, stmt);
                }

                // Look for a ';'. If we are dealing with a one line test statment
                // the delimiter will in practice be ';' and if it is a multi-line
                // test statement then the test-script delimiter will be something
                // else than ';' and ';' will be the delimiter used in the multi-line
                // statement.
                i = line.find(";");

                if (i != string::npos)
                {
                    // Is there a "-- " or "#" after the delimiter?
                    if ((line.find("-- ", i) != string::npos)
                        || (line.find("#", i) != string::npos))
                    {
                        if (is_postgres())
                        {
                            found = true;
                            continue;
                        }
                        else
                        {
                            // If so, add a newline. Otherwise the rest of the
                            // statement would be included in the comment.
                            stmt += "\n";
                        }
                    }

                    // This is somewhat fragile as a ";", "#" or "-- " inside a
                    // string will trigger this behaviour...
                }

                string c;

                if (line.length() >= m_delimiter.length())
                {
                    c = line.substr(line.length() - m_delimiter.length());
                }

                if (c == m_delimiter)
                {
                    if (c != ";")
                    {
                        // If the delimiter was something else but ';' we need to
                        // remove that before giving the line to the classifiers.
                        stmt.erase(stmt.length() - m_delimiter.length());
                    }

                    if (!skip)
                    {
                        found = true;
                    }
                    else
                    {
                        skip = false;
                        stmt.clear();
                    }
                }
                else if (!skip)
                {
                    stmt += " ";
                }
            }
            else if (line.substr(0, 7) == "--error")
            {
                // Next statement is supposed to fail, no need to check.
                skip = true;
            }
        }

        if (!error && is_postgres() && postgres_skip)
        {
            stmt.clear();
            found = false;
        }
    }
    while (!error && !found && stmt.empty() && m_in);

    result_t result;

    if (error)
    {
        result = RESULT_ERROR;
    }
    else if (found)
    {
        result = RESULT_STMT;
    }
    else
    {
        result = RESULT_EOF;
    }

    return result;
}

// static
void TestReader::init()
{
    static bool inited = false;

    if (!inited)
    {
        inited = true;

        init_keywords();
    }
}

void TestReader::skip_block()
{
    int c;

    // Find first '{'
    while (m_in && ((c = m_in.get()) != '{'))
    {
        if (c == '\n')
        {
            ++m_line;
        }
    }

    int n = 1;

    while ((n > 0) && m_in)
    {
        c = m_in.get();

        switch (c)
        {
        case '{':
            ++n;
            break;

        case '}':
            --n;
            break;

        case '\n':
            ++m_line;
            break;

        default:
            ;
        }
    }
}

void TestReader::skip_postgres_block_quote(std::string& line)
{
    line.clear();

    string l;
    while (std::getline(m_in, l))
    {
        ++m_line;

        auto i = l.find("*/");
        if (i != string::npos)
        {
            line = l.substr(i + 2);
            break;
        }
    }
}

void TestReader::skip_postgres_dollar_quotes(std::string& line, std::string& stmt)
{
    // When an '$$' is encountered, ignore all ';' until next '$$'.
    auto i = line.find("$$");

    if (i != string::npos)
    {
        if (i == line.length() - 2)
        {
            stmt += "\n";
        }

        string l = line.substr(i + 2);
        i = l.find("$$");

        if (i != string::npos)
        {
            // We found the end
            line = l.substr(i + 2);
        }
        else
        {
            while (std::getline(m_in, l))
            {
                ++m_line;

                stmt += l;
                stmt += "\n";

                i = l.find("$$");

                if (i != string::npos)
                {
                    break;
                }
            }

            if (i != string::npos)
            {
                line = l.substr(i + 2);
            }
            else
            {
                line.clear();
            }
        }
    }
}

void TestReader::skip_postgres_stdin_input()
{
    string line;
    while (std::getline(m_in, line))
    {
        ++m_line;

        mxb::ltrim(line);

        if (line.substr(0, 2) == "\\.")
        {
            break;
        }
    }
}

void TestReader::skip_postgres_until_ok()
{
    string line;
    while (std::getline(m_in, line))
    {
        ++m_line;

        mxb::ltrim(line);
        mxb::lower_case(line);

        // If '-- fail' or '-- bogus' encountered again...
        if (line.substr(0, 7) == "-- fail" || line.substr(0, 8) == "-- bogus")
        {
            // ...continue ignoring.
            continue;
        }
        else if (line.substr(0, 6) == "--")
        {
            // Stop ignoring at any other '--' line.
            break;
        }
    }
}

}
