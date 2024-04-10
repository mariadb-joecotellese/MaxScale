/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "pam_backend_session.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/protocol/mariadb/protocol_classes.hh>

using std::string;

/**
 * Parse prompt type and message text from packet data. Advances pointer.
 *
 * @param data Data from server. The pointer is advanced.
 * @return True if all expected fields were parsed
 */
PamBackendAuthenticator::PromptType
PamBackendAuthenticator::parse_password_prompt(mariadb::ByteVec& data)
{
    if (data.size() < 2)    // Need at least message type + message
    {
        return PromptType::FAIL;
    }

    data.push_back('\0');   // Simplifies parsing by ensuring 0-termination.
    const uint8_t* ptr = data.data();
    const char* server_name = m_shared_data.servername;
    auto* client_data = m_shared_data.client_data;
    auto pw_type = PromptType::FAIL;
    int msg_type = *ptr++;
    if (msg_type == DIALOG_ECHO_ENABLED || msg_type == DIALOG_ECHO_DISABLED)
    {
        const char* messages = reinterpret_cast<const char*>(ptr);
        // The rest of the buffer contains a message.
        // The server separates messages with linebreaks. Search for the last.
        const char* linebrk_pos = strrchr(messages, '\n');
        const char* prompt;
        if (linebrk_pos)
        {
            int msg_len = linebrk_pos - messages;
            MXB_INFO("'%s' sent message when authenticating %s: %.*s",
                     server_name, client_data->user_and_host().c_str(), msg_len, messages);
            prompt = linebrk_pos + 1;
        }
        else
        {
            prompt = messages;      // No additional messages.
        }

        // If using normal password-only authentication, expect the server to only ask for "Password: ".
        if (m_mode == AuthMode::PW)
        {
            if (mxb::pam::match_prompt(prompt, mxb::pam::EXP_PW_QUERY))
            {
                pw_type = PromptType::PASSWORD;
            }
            else
            {
                MXB_ERROR("'%s' asked for '%s' when authenticating %s. '%s' was expected.",
                          server_name, prompt, client_data->user_and_host().c_str(),
                          mxb::pam::EXP_PW_QUERY.c_str());
            }
        }
        else
        {
            // In two-factor mode, any non "Password" prompt is assumed to ask for the 2FA-code.
            pw_type = (mxb::pam::match_prompt(prompt, mxb::pam::EXP_PW_QUERY)) ? PromptType::PASSWORD :
                PromptType::TWO_FA;
        }
    }
    else
    {
        MXB_ERROR("'%s' sent an unknown message type %i when authenticating %s.",
                  server_name, msg_type, client_data->user_and_host().c_str());
    }
    return pw_type;
}

/**
 * Generate packet with client password in cleartext.
 *
 * @return Packet with password
 */
GWBUF PamBackendAuthenticator::generate_pw_packet(PromptType pw_type) const
{
    const auto& source = (pw_type == PromptType::PASSWORD) ?
        m_shared_data.client_data->auth_data->backend_token :
        m_shared_data.client_data->auth_data->backend_token_2fa;

    auto auth_token_len = source.size();
    size_t buflen = MYSQL_HEADER_LEN + auth_token_len;
    GWBUF rval(buflen);
    auto* ptr = rval.data();
    ptr = mariadb::write_header(ptr, auth_token_len, m_sequence);
    if (auth_token_len > 0)
    {
        ptr = mariadb::copy_bytes(ptr, source.data(), auth_token_len);
    }
    mxb_assert(ptr - rval.data() == (ptrdiff_t)buflen);
    return rval;
}

mariadb::BackendAuthenticator::AuthRes
PamBackendAuthenticator::exchange(GWBUF&& input)
{
    /**
     * The server PAM plugin sends data usually once, at the moment it gets a prompt-type message
     * from the api. The "message"-segment may contain multiple messages from the api separated by \n.
     * MaxScale should ignore this text and search for "Password: " near the end of the message. See
     * https://github.com/MariaDB/server/blob/10.3/plugin/auth_pam/auth_pam.c
     * for how communication is handled on the other side.
     *
     * The AuthSwitchRequest packet:
     * 4 bytes     - Header
     * 0xfe        - Command byte
     * string[NUL] - Auth plugin name, should be "dialog"
     * byte        - Message type, 2 or 4
     * string[EOF] - Message(s)
     *
     * Additional prompts after AuthSwitchRequest:
     * 4 bytes     - Header
     * byte        - Message type, 2 or 4
     * string[EOF] - Message(s)
     *
     * Authenticators receive complete packets from protocol.
     */

    const char* srv_name = m_shared_data.servername;
    // Smallest buffer that is parsed, header + (cmd-byte/msg-type + message).
    const int min_readable_buflen = MYSQL_HEADER_LEN + 1 + 1;
    // The buffer should be of reasonable size. Large buffers likely mean that the auth scheme is complicated.
    const int MAX_BUFLEN = 2000;
    const auto buflen = input.length();
    if (buflen <= min_readable_buflen || buflen > MAX_BUFLEN)
    {
        MXB_ERROR("Received packet of size %lu from '%s' during authentication. Expected packet size is "
                  "between %i and %i.", buflen, srv_name, min_readable_buflen, MAX_BUFLEN);
        return {false, GWBUF()};
    }

    m_sequence = MYSQL_GET_PACKET_NO(input.data()) + 1;
    AuthRes rval;

    switch (m_state)
    {
    case State::EXPECT_AUTHSWITCH:
        {
            // Server should have sent the AuthSwitchRequest. If server version is 10.4, the server may not
            // send a prompt. Older versions add the first prompt to the same packet.
            auto parse_res = mariadb::parse_auth_switch_request(input);
            if (parse_res.success)
            {
                // Support both "dialog" and "mysql_clear_password".
                if (parse_res.plugin_name == DIALOG)
                {
                    if (parse_res.plugin_data.empty())
                    {
                        // Just the AuthSwitchRequest, this is ok. The server now expects a password.
                        rval.output = generate_pw_packet(PromptType::PASSWORD);
                        m_state = State::EXCHANGING;
                        rval.success = true;
                    }
                    else
                    {
                        auto pw_type = parse_password_prompt(parse_res.plugin_data);
                        if (pw_type != PromptType::FAIL)
                        {
                            // Got a password prompt, send answer.
                            rval.output = generate_pw_packet(pw_type);
                            m_state = State::EXCHANGING;
                            rval.success = true;
                        }
                    }
                }
                else if (parse_res.plugin_name == CLEAR_PW)
                {
                    rval.output = generate_pw_packet(PromptType::PASSWORD);
                    m_state = State::EXCHANGE_DONE;     // Server should not ask for anything else.
                    rval.success = true;
                }
                else
                {
                    const char msg[] = "'%s' asked for authentication plugin '%s' when authenticating %s. "
                                       "Only '%s' and '%s' are supported.";
                    MXB_ERROR(msg, m_shared_data.servername, parse_res.plugin_name.c_str(),
                              m_shared_data.client_data->user_and_host().c_str(),
                              DIALOG.c_str(), CLEAR_PW.c_str());
                }
            }
            else
            {
                // No AuthSwitchRequest, error.
                MXB_ERROR(MALFORMED_AUTH_SWITCH, m_shared_data.servername);
            }
        }
        break;

    case State::EXCHANGING:
        {
            // The packet may contain another prompt, try parse it.
            mariadb::ByteVec data;
            data.reserve(input.length());   // reserve some extra to ensure no further allocations needed.
            size_t datalen = input.length() - MYSQL_HEADER_LEN;
            data.resize(datalen);
            input.copy_data(MYSQL_HEADER_LEN, datalen, data.data());

            auto pw_type = parse_password_prompt(data);
            if (pw_type != PromptType::FAIL)
            {
                rval.output = generate_pw_packet(pw_type);
                rval.success = true;
            }
        }
        break;

    case State::EXCHANGE_DONE:
        // Server is acting weird, error. Likely a misconfigured pam setup.
        MXB_ERROR("'%s' sent an unexpected message during authentication, possibly due to a misconfigured "
                  "PAM setup.", m_shared_data.servername);
        break;

    case State::ERROR:
        // Should not get here.
        mxb_assert(!true);
        break;
    }

    if (!rval.success)
    {
        m_state = State::ERROR;
    }
    return rval;
}

PamBackendAuthenticator::PamBackendAuthenticator(mariadb::BackendAuthData& shared_data, AuthMode mode)
    : m_shared_data(shared_data)
    , m_mode(mode)
{
}
