/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#define MXB_MODULE_NAME "hintfilter"

#include <unordered_map>

#include <maxscale/filter.hh>
#include <maxscale/buffer.hh>
#include <maxscale/protocol/mariadb/mysql.hh>

#include "mysqlhint.hh"

/**
 * Code for parsing SQL comments and processing them into MaxScale hints
 */

using InputIter = HintParser::InputIter;

/**
 * Advance an iterator until either an unescaped character `c` is found or `end` is reached
 *
 * @param it  Iterator to start from
 * @param end Past-the-end iterator
 * @param c   The character to look for
 *
 * @return The iterator pointing at the first occurrence of the character or `end` if one was not found
 */
InputIter skip_until(InputIter it, InputIter end, char c)
{
    while (it != end)
    {
        if (*it == '\\')
        {
            if (++it == end)
            {
                continue;
            }
        }
        else if (*it == c)
        {
            break;
        }

        ++it;
    }

    return it;
}

/**
 * Extract a MariaDB comment
 *
 * @param it  Iterator pointing to the start of the search string
 * @param end Past-the-end iterator
 *
 * @return A pair of iterators pointing to the range the comment spans. The comment tags themselves are not
 *         included in this range. If no comment is found, a pair of `end` iterators is returned.
 */
std::pair<InputIter, InputIter> get_comment(InputIter it, InputIter end)
{
    while (it != end)
    {
        switch (*it)
        {
        case '\\':
            if (++it == end)
            {
                continue;
            }
            break;

        case '"':
        case '\'':
        case '`':
            // Quoted literal string or identifier
            if ((it = skip_until(std::next(it), end, *it)) == end)
            {
                // Malformed quoted value
                continue;
            }
            break;

        case '#':
            // A comment that spans the rest of the line
            ++it;
            return {it, skip_until(it, end, '\n')};

        case '-':
            if (++it != end && *it == '-' && ++it != end && *it == ' ')
            {
                ++it;
                return {it, skip_until(it, end, '\n')};
            }
            continue;

        case '/':
            if (++it != end && *it == '*' && ++it != end)
            {
                auto start = it;

                while (it != end)
                {
                    auto comment_end = skip_until(it, end, '*');
                    it = comment_end;

                    if (it != end && ++it != end && *it == '/')
                    {
                        return {start, comment_end};
                    }
                }
            }
            continue;

        default:
            break;
        }

        ++it;
    }

    return {end, end};
}

/**
 * Extract all MariaDB comments from a query
 *
 * @param start Iterator position to start from
 * @param end   Past-the-end iterator
 *
 * @return A list of iterator pairs pointing to all comments in the query
 */
std::vector<std::pair<InputIter, InputIter>> get_all_comments(InputIter start, InputIter end)
{
    std::vector<std::pair<InputIter, InputIter>> rval;

    do
    {
        auto comment = get_comment(start, end);

        if (comment.first != comment.second)
        {
            rval.push_back(comment);
        }

        start = comment.second;
    }
    while (start != end);

    return rval;
}

static const std::unordered_map<std::string, TOKEN_VALUE> tokens
{
    {"begin", TOK_START},
    {"end", TOK_STOP},
    {"last", TOK_LAST},
    {"master", TOK_MASTER},
    {"maxscale", TOK_MAXSCALE},
    {"prepare", TOK_PREPARE},
    {"route", TOK_ROUTE},
    {"server", TOK_SERVER},
    {"slave", TOK_SLAVE},
    {"start", TOK_START},
    {"stop", TOK_STOP},
    {"to", TOK_TO},
};

/**
 * Extract the next token
 *
 * @param it  Iterator to start from, modified to point to next non-whitespace character after the token
 * @param end Past-the-end iterator
 *
 * @return The next token
 */
TOKEN_VALUE HintParser::next_token()
{
    while (m_it != m_end && isspace(*m_it))
    {
        ++m_it;
    }

    TOKEN_VALUE type = TOK_END;
    m_tok_begin = m_it;

    if (m_it != m_end)
    {
        if (*m_it == '=')
        {
            ++m_it;
            type = TOK_EQUAL;
        }
        else
        {
            while (m_it != m_end && !isspace(*m_it) && *m_it != '=')
            {
                ++m_it;
            }

            auto t = tokens.find(std::string(m_tok_begin, m_it));

            if (t != tokens.end())
            {
                type = t->second;
            }
        }

        if (type == TOK_END && m_tok_begin != m_it)
        {
            // We read a string identifier
            type = TOK_STRING;
        }
    }

    m_tok_end = m_it;

    return type;
}

/**
 * Process the definition of a hint
 *
 * @param it  Start iterator
 * @param end End iterator
 *
 * @return The processed hint or HINT_NONE on invalid input
 */
Hint HintParser::process_definition()
{
    Hint rval;
    auto t = next_token();

    if (t == TOK_ROUTE)
    {
        if (next_token() == TOK_TO)
        {
            t = next_token();

            if (t == TOK_MASTER)
            {
                rval = Hint(Hint::Type::ROUTE_TO_MASTER);
            }
            else if (t == TOK_SLAVE)
            {
                rval = Hint(Hint::Type::ROUTE_TO_SLAVE);
            }
            else if (t == TOK_LAST)
            {
                rval = Hint(Hint::Type::ROUTE_TO_LAST_USED);
            }
            else if (t == TOK_SERVER)
            {
                if (next_token() == TOK_STRING)
                {
                    std::string value(m_tok_begin, m_tok_end);
                    rval = Hint(Hint::Type::ROUTE_TO_NAMED_SERVER, value);
                }
            }
        }
    }
    else if (t == TOK_STRING)
    {
        std::string key(m_tok_begin, m_tok_end);
        auto eq = next_token();
        auto val = next_token();

        if (eq == TOK_EQUAL && val == TOK_STRING)
        {
            std::string value(m_tok_begin, m_tok_end);
            rval = Hint(key, value);
        }
    }

    if (rval && next_token() != TOK_END)
    {
        // Unexpected input after hint definition, treat it as an error and remove the hint
        rval = Hint();
    }

    return rval;
}

Hint HintParser::parse_one(InputIter it, InputIter end)
{
    m_it = it;
    m_end = end;
    Hint rval;

    if (next_token() == TOK_MAXSCALE)
    {
        // Peek at the next token
        auto prev_it = m_it;
        auto t = next_token();

        if (t == TOK_START)
        {
            if ((rval = process_definition()))
            {
                m_stack.push_back(rval);
            }
        }
        else if (t == TOK_STOP)
        {
            if (!m_stack.empty())
            {
                m_stack.pop_back();
            }
        }
        else if (t == TOK_STRING)
        {
            std::string key(m_tok_begin, m_tok_end);
            t = next_token();

            if (t == TOK_EQUAL)
            {
                if (next_token() == TOK_STRING)
                {
                    // A key=value hint
                    std::string value(m_tok_begin, m_tok_end);
                    rval = Hint(key, value);
                }
            }
            else if (t == TOK_PREPARE)
            {
                Hint hint = process_definition();
                if (hint)
                {
                    // Preparation of a named hint
                    m_named_hints[key] = hint;
                }
            }
            else if (t == TOK_START)
            {
                if ((rval = process_definition()))
                {
                    if (m_named_hints.count(key) == 0)
                    {
                        // New hint defined, push it on to the stack
                        m_named_hints[key] = rval;
                        m_stack.push_back(rval);
                    }
                }
                else if (next_token() == TOK_END)
                {
                    auto it2 = m_named_hints.find(key);

                    if (it2 != m_named_hints.end())
                    {
                        // We're starting an already define named hint
                        m_stack.push_back(it2->second);
                        rval = it2->second;
                    }
                }
            }
        }
        else
        {
            // Only hint definition in the comment, rewind the iterator to process it again
            m_it = prev_it;
            rval = process_definition();
        }
    }

    return rval;
}

HintParser::HintVector HintParser::parse(InputIter it, InputIter end)
{
    HintVector rval;

    for (const auto& comment : get_all_comments(it, end))
    {
        Hint hint = parse_one(comment.first, comment.second);
        if (hint)
        {
            rval.push_back(std::move(hint));
        }
    }

    if (rval.empty() && !m_stack.empty())
    {
        rval.push_back(m_stack.back());
    }

    return rval;
}

uint32_t HintSession::get_id(const GWBUF& buffer) const
{
    auto ps_id = mxs_mysql_extract_ps_id(buffer);

    if (ps_id == MARIADB_PS_DIRECT_EXEC_ID && m_prev_id)
    {
        ps_id = m_prev_id;
    }

    return ps_id;
}

std::vector<Hint> HintSession::process_hints(const GWBUF& buffer)
{
    HintParser::HintVector hints;
    uint8_t cmd = mariadb::get_command(buffer);

    if (cmd == MXS_COM_QUERY)
    {
        hints = m_parser.parse(std::next(buffer.begin(), 5), buffer.end());
    }
    else if (cmd == MXS_COM_STMT_PREPARE)
    {
        auto tmp = m_parser.parse(std::next(buffer.begin(), 5), buffer.end());
        if (!tmp.empty())
        {
            uint32_t id = buffer.id();
            mxb_assert(id != 0);
            mxb_assert(m_ps.find(id) == m_ps.end());

            // We optimistically assume that the prepared statement will be successful and store it in the
            // map. If it doesn't, we'll erase it when we get the error. The client protocol guarantees that
            // only one binary protocol prepared statement is executed at a time.
            m_ps.emplace(id, std::move(tmp));
            m_current_id = id;
            m_prev_id = id;
        }
    }
    else if (cmd == MXS_COM_STMT_CLOSE)
    {
        m_ps.erase(get_id(buffer));
    }
    else if (mxs_mysql_is_ps_command(cmd))
    {
        auto it = m_ps.find(get_id(buffer));

        if (it != m_ps.end())
        {
            hints = it->second;
        }
    }

    return hints;
}
