/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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

#include "nosqlcommands.hh"
#include <maxbase/string.hh>
#include <maxbase/worker.hh>

//
// Files that contain no implemented commands are commented out.
//
#include "commands/aggregation.hh"
//#include "commands/geospatial.hh"
#include "commands/query_and_write_operation.hh"
//#include "commands/query_plan_cache.hh"
#include "commands/authentication.hh"
#include "commands/user_management.hh"
//#include "commands/role_management.hh"
#include "commands/replication.hh"
//#include "commands/sharding.hh"
#include "commands/sessions.hh"
#include "commands/administration.hh"
#include "commands/diagnostic.hh"
#include "commands/free_monitoring.hh"
//#include "commands/system_events_auditing.hh"

#include "commands/sasl.hh"
#include "commands/maxscale.hh"

using namespace std;

namespace
{

class Unknown : public nosql::ImmediateCommand
{
public:
    using nosql::ImmediateCommand::ImmediateCommand;

    Response::Status populate_response(nosql::DocumentBuilder& doc) override
    {
        if (m_database.config().log_unknown_command)
        {
            MXB_WARNING("Unknown command: %s", bsoncxx::to_json(m_doc).c_str());
        }

        string command;
        if (!m_doc.empty())
        {
            auto element = *m_doc.begin();
            auto key = element.key();
            command = string(key.data(), key.length());
        }

        ostringstream ss;
        ss << "no such command: '" << command << "'";
        auto s = ss.str();

        switch (m_database.config().on_unknown_command)
        {
        case Configuration::RETURN_ERROR:
            throw nosql::SoftError(s, nosql::error::COMMAND_NOT_FOUND);
            break;

        case Configuration::RETURN_EMPTY:
            break;
        }

        return Response::Status::NOT_CACHEABLE;
    }
};

using namespace nosql;

template<class ConcreteCommand>
unique_ptr<OpMsgCommand> create_default_command(const string& name,
                                                Database* pDatabase,
                                                GWBUF* pRequest,
                                                packet::Msg&& msg)
{
    unique_ptr<ConcreteCommand> sCommand;

    sCommand.reset(new ConcreteCommand(name, pDatabase, pRequest, std::move(msg)));

    return sCommand;
}

template<class ConcreteCommand>
unique_ptr<OpMsgCommand> create_diagnose_command(const string& name,
                                                 Database* pDatabase,
                                                 GWBUF* pRequest,
                                                 packet::Msg&& msg,
                                                 const bsoncxx::document::view& doc,
                                                 const OpMsgCommand::DocumentArguments& arguments)
{
    unique_ptr<ConcreteCommand> sCommand;

    sCommand.reset(new ConcreteCommand(name, pDatabase, pRequest, std::move(msg), doc, arguments));

    return sCommand;
}

template<class ConcreteCommand>
OpMsgCommandInfo create_info()
{
    return OpMsgCommandInfo(ConcreteCommand::KEY,
                            ConcreteCommand::HELP,
                            ConcreteCommand::IS_CACHEABLE,
                            command::IsAdmin<ConcreteCommand>::is_admin,
                            &create_default_command<ConcreteCommand>,
                            &create_diagnose_command<ConcreteCommand>);
}

using InfosByName = const map<string, OpMsgCommandInfo>;

struct ThisUnit
{
    static std::string tolower(const char* zString)
    {
        return mxb::tolower(zString);
    }

    InfosByName infos_by_name =
    {
        // NOTE: This *MUST* be kept in alphabetical order.
        { tolower(command::BuildInfo::KEY),                create_info<command::BuildInfo>() },
        { tolower(command::Count::KEY),                    create_info<command::Count>() },
        { tolower(command::Create::KEY),                   create_info<command::Create>() },
        { tolower(command::CreateIndexes::KEY),            create_info<command::CreateIndexes>() },
        { tolower(command::CreateUser::KEY),               create_info<command::CreateUser>() },
        //Cannot be included as a mockup, causes hangs.
        //{ tolower(command::CurrentOp::KEY),                create_info<command::CurrentOp>() },
        { tolower(command::Delete::KEY),                   create_info<command::Delete>() },
        { tolower(command::Distinct::KEY),                 create_info<command::Distinct>() },
        { tolower(command::Drop::KEY),                     create_info<command::Drop>() },
        { tolower(command::DropAllUsersFromDatabase::KEY), create_info<command::DropAllUsersFromDatabase>() },
        { tolower(command::DropDatabase::KEY),             create_info<command::DropDatabase>() },
        { tolower(command::DropIndexes::KEY),              create_info<command::DropIndexes>() },
        { tolower(command::DropUser::KEY),                 create_info<command::DropUser>() },
        { tolower(command::EndSessions::KEY),              create_info<command::EndSessions>() },
        { tolower(command::Explain::KEY),                  create_info<command::Explain>() },
        { tolower(command::FSync::KEY),                    create_info<command::FSync>() },
        { tolower(command::Find::KEY),                     create_info<command::Find>() },
        { tolower(command::FindAndModify::KEY),            create_info<command::FindAndModify>() },
        { tolower(command::GetCmdLineOpts::KEY),           create_info<command::GetCmdLineOpts>() },
        { tolower(command::GetFreeMonitoringStatus::KEY),  create_info<command::GetFreeMonitoringStatus>() },
        { tolower(command::GetLastError::KEY),             create_info<command::GetLastError>() },
        { tolower(command::GetLog::KEY),                   create_info<command::GetLog>() },
        { tolower(command::GetMore::KEY),                  create_info<command::GetMore>() },
        { tolower(command::GrantRolesToUser::KEY),         create_info<command::GrantRolesToUser>() },
        { tolower(command::HostInfo::KEY),                 create_info<command::HostInfo>() },
        { tolower(command::Insert::KEY),                   create_info<command::Insert>() },
        { tolower(command::IsMaster::KEY),                 create_info<command::IsMaster>() },
        { tolower(command::KillCursors::KEY),              create_info<command::KillCursors>() },
        { tolower(command::ListCollections::KEY),          create_info<command::ListCollections>() },
        { tolower(command::ListCommands::KEY),             create_info<command::ListCommands>() },
        { tolower(command::ListDatabases::KEY),            create_info<command::ListDatabases>() },
        { tolower(command::ListIndexes::KEY),              create_info<command::ListIndexes>() },
        { tolower(command::Logout::KEY),                   create_info<command::Logout>() },
        { tolower(command::MxsAddUser::KEY),               create_info<command::MxsAddUser>() },
        { tolower(command::MxsCreateDatabase::KEY),        create_info<command::MxsCreateDatabase>() },
        { tolower(command::MxsDiagnose::KEY),              create_info<command::MxsDiagnose>() },
        { tolower(command::MxsGetConfig::KEY),             create_info<command::MxsGetConfig>() },
        { tolower(command::MxsRemoveUser::KEY),            create_info<command::MxsRemoveUser>() },
        { tolower(command::MxsSetConfig::KEY),             create_info<command::MxsSetConfig>() },
        { tolower(command::MxsUpdateUser::KEY),            create_info<command::MxsUpdateUser>() },
        { tolower(command::Ping::KEY),                     create_info<command::Ping>() },
        { tolower(command::RenameCollection::KEY),         create_info<command::RenameCollection>() },
        { tolower(command::ReplSetGetStatus::KEY),         create_info<command::ReplSetGetStatus>() },
        { tolower(command::ResetError::KEY),               create_info<command::ResetError>() },
        { tolower(command::RevokeRolesFromUser::KEY),      create_info<command::RevokeRolesFromUser>() },
        { tolower(command::ServerStatus::KEY),             create_info<command::ServerStatus>() },
        { tolower(command::SaslContinue::KEY),             create_info<command::SaslContinue>() },
        { tolower(command::SaslStart::KEY),                create_info<command::SaslStart>() },
        { tolower(command::SetParameter::KEY),             create_info<command::SetParameter>() },
        { tolower(command::Update::KEY),                   create_info<command::Update>() },
        { tolower(command::UpdateUser::KEY),               create_info<command::UpdateUser>() },
        { tolower(command::UsersInfo::KEY),                create_info<command::UsersInfo>() },
        { tolower(command::Validate::KEY),                 create_info<command::Validate>() },
        { tolower(command::WhatsMyUri::KEY),               create_info<command::WhatsMyUri>() },
    };
} this_unit;

}

namespace nosql
{

//
// OpDeleteCommand
//
std::string OpDeleteCommand::description() const
{
    return "OP_DELETE";
}

State OpDeleteCommand::execute(Response* pNoSQL_response)
{
    ostringstream ss;
    ss << "DELETE FROM " << table() << where_clause_from_query(m_req.selector()) << " ";

    if (m_req.is_single_remove())
    {
        ss << "LIMIT 1";
    }

    auto statement = ss.str();

    send_downstream(statement);

    return State::BUSY;
}

State OpDeleteCommand::translate(GWBUF&& mariadb_response, Response* pNoSQL_response)
{
    ComResponse response(mariadb_response.data());

    switch (response.type())
    {
    case ComResponse::OK_PACKET:
        {
            ComOK ok(response);

            m_database.context().set_last_error(std::make_unique<NoError>(ok.affected_rows(), true));
        }
        break;

    case ComResponse::ERR_PACKET:
        {
            ComERR err(response);

            if (err.code() != ER_NO_SUCH_TABLE)
            {
                m_database.context().set_last_error(MariaDBError(err).create_last_error());
            }
            else
            {
                m_database.context().set_last_error(std::make_unique<NoError>(0));
            }
        }
        break;

    default:
        throw_unexpected_packet();
    }

    return State::READY;
};

//
// OpInsertCommand
//
std::string OpInsertCommand::description() const
{
    return "OP_INSERT";
}

State OpInsertCommand::execute(Response* pNoSQL_response)
{
    mxb_assert(m_req.documents().size() == 1);

    if (m_req.documents().size() != 1)
    {
        const char* zMessage = "Currently only a single document can be insterted at a time with OP_INSERT.";
        MXB_ERROR("%s", zMessage);

        throw HardError(zMessage, error::INTERNAL_ERROR);
    }

    auto doc = m_req.documents()[0];

    ostringstream ss;
    ss << "INSERT INTO " << table() << " (doc) VALUES " << convert_document_data(doc) << ";";

    m_statement = ss.str();

    send_downstream(m_statement);

    return State::BUSY;
}

State OpInsertCommand::translate2(GWBUF&& mariadb_response, GWBUF** ppNoSQL_response)
{
    State state = State::BUSY;
    GWBUF* pResponse = nullptr;

    ComResponse response(mariadb_response.data());

    switch (response.type())
    {
    case ComResponse::OK_PACKET:
        m_database.context().set_last_error(std::make_unique<NoError>(1));
        state = State::READY;
        break;

    case ComResponse::ERR_PACKET:
        {
            ComERR err(response);
            auto s = err.message();

            switch (err.code())
            {
            case ER_NO_SUCH_TABLE:
                create_table();
                break;

            default:
                throw MariaDBError(err);
            }
        }
        break;

    default:
        mxb_assert(!true);
        throw_unexpected_packet();
    }

    *ppNoSQL_response = pResponse;
    return state;
};

State OpInsertCommand::table_created(GWBUF** ppResponse)
{
    send_downstream_via_loop(m_statement);

    *ppResponse = nullptr;
    return State::BUSY;
}

string OpInsertCommand::convert_document_data(const bsoncxx::document::view& doc)
{
    ostringstream sql;

    string json;

    auto element = doc["_id"];

    if (element)
    {
        json = bsoncxx::to_json(doc);
    }
    else
    {
        // Ok, as the document does not have an id, one must be generated. However,
        // as an existing document is immutable, a new one must be created.

        bsoncxx::oid oid;

        DocumentBuilder builder;
        builder.append(kvp(key::_ID, oid));

        for (const auto& e : doc)
        {
            append(builder, e.key(), e);
        }

        // We need to keep the created document around, so that 'element'
        // down below stays alive.
        m_stashed_documents.emplace_back(builder.extract());

        const auto& doc_with_id = m_stashed_documents.back();

        json = bsoncxx::to_json(doc_with_id);
    }

    json = escape_essential_chars(std::move(json));

    sql << "('" << json << "')";

    return sql.str();
}

//
// OpUpdateCommand
//
OpUpdateCommand::~OpUpdateCommand()
{
}

string OpUpdateCommand::description() const
{
    return "OP_UPDATE";
}

State OpUpdateCommand::execute(Response* pNoSQL_response)
{
    ostringstream ss;
    ss << "UPDATE " << table() << " SET DOC = "
       << set_value_from_update_specification(m_req.update()) << " "
       << where_clause_from_query(m_req.selector()) << " ";

    if (!m_req.is_multi())
    {
        ss << "LIMIT 1";
    }

    update_document(ss.str(), Send::DIRECTLY);

    return State::BUSY;
}

State OpUpdateCommand::translate2(GWBUF&& mariadb_response, GWBUF** ppNoSQL_response)
{
    State state = State::READY;

    ComResponse response(mariadb_response.data());

    auto type = response.type();
    if (type == ComResponse::OK_PACKET || type == ComResponse::ERR_PACKET)
    {
        switch (m_action)
        {
        case Action::UPDATING_DOCUMENT:
            state = translate_updating_document(response);
            break;

        case Action::INSERTING_DOCUMENT:
            state = translate_inserting_document(response);
            break;
        }
    }
    else
    {
        throw_unexpected_packet();
    }

    *ppNoSQL_response = nullptr;

    return state;
}

State OpUpdateCommand::translate_updating_document(ComResponse& response)
{
    State state = State::READY;

    if (response.type() == ComResponse::OK_PACKET)
    {
        ComOK ok(response);

        if (ok.matched_rows() == 0)
        {
            if (m_req.is_upsert())
            {
                if (m_insert.empty())
                {
                    // We have not attempted an insert, so let's do that.
                    state = insert_document();
                }
                else
                {
                    // An insert has been made, but now the update did not match?!

                    SoftError error("The query did not match a document, and a document "
                                    "was thus inserted, but yet there was no match.",
                                    error::COMMAND_FAILED);

                    m_database.context().set_last_error(error.create_last_error());
                }
            }
            else
            {
                m_database.context().set_last_error(std::make_unique<NoError>(0, false));
            }
        }
        else
        {
            auto n = ok.affected_rows();

            if (n == 0)
            {
                m_database.context().set_last_error(std::make_unique<NoError>(0, false));
            }
            else
            {
                if (m_insert.empty())
                {
                    // We did not try inserting anything, which means something existing was updated.
                    m_database.context().set_last_error(std::make_unique<NoError>(n, true));
                }
                else
                {
                    // Ok, so we updated an inserted document.
                    m_database.context().set_last_error(std::make_unique<NoError>(std::move(m_sId)));
                }
            }
        }
    }
    else
    {
        mxb_assert(response.type() == ComResponse::ERR_PACKET);

        ComERR err(response);

        if (err.code() == ER_NO_SUCH_TABLE)
        {
            create_table();
            state = State::BUSY;
        }
        else
        {
            throw MariaDBError(err);
        }
    }

    return state;
}

State OpUpdateCommand::translate_inserting_document(ComResponse& response)
{
    if (response.type() == ComResponse::ERR_PACKET)
    {
        throw MariaDBError(ComERR(response));
    }
    else
    {
        ostringstream ss;
        ss << "UPDATE " << table() << " SET DOC = "
           << set_value_from_update_specification(m_req.update())
           << " "
           << "WHERE id = '" << m_sId->to_string() << "'";

        update_document(ss.str(), Send::VIA_LOOP);
    }

    return State::BUSY;
}

State OpUpdateCommand::table_created(GWBUF** ppResponse)
{
    insert_document();

    *ppResponse = nullptr;
    return State::BUSY;
}

void OpUpdateCommand::update_document(const string& sql, Send send)
{
    m_action = Action::UPDATING_DOCUMENT;

    m_update = sql;

    if (send == Send::DIRECTLY)
    {
        send_downstream(m_update);
    }
    else
    {
        send_downstream_via_loop(m_update);
    }
}

State OpUpdateCommand::insert_document()
{
    m_action = Action::INSERTING_DOCUMENT;

    ostringstream ss;
    ss << "INSERT INTO " << table() << " (doc) VALUES ('";

    auto q = m_req.selector();

    DocumentBuilder builder;

    auto qid = q[key::_ID];

    if (qid)
    {
        class ElementId : public NoError::Id
        {
        public:
            ElementId(const bsoncxx::document::element& id)
                : m_id(id)
            {
            }

            string to_string() const override
            {
                return nosql::to_string(m_id);
            }

            void append(DocumentBuilder& doc, const string& key) const override
            {
                nosql::append(doc, key, m_id);
            }

        private:
            bsoncxx::document::element m_id;
        };

        m_sId = make_unique<ElementId>(qid);
    }
    else
    {
        auto id = bsoncxx::oid();

        class ObjectId : public NoError::Id
        {
        public:
            ObjectId(const bsoncxx::oid& id)
                : m_id(id)
            {
            }

            string to_string() const override
            {
                return "{\"$oid\":\"" + m_id.to_string() + "\"}'";
            }

            void append(DocumentBuilder& doc, const string& key) const override
            {
                doc.append(kvp(key, m_id));
            }

        private:
            bsoncxx::oid m_id;
        };

        m_sId = make_unique<ObjectId>(id);

        builder.append(kvp(key::_ID, id));
    }

    for (const auto& e : q)
    {
        append(builder, e.key(), e);
    }

    ss << bsoncxx::to_json(builder.extract());

    ss << "')";

    m_insert = ss.str();

    send_downstream_via_loop(m_insert);

    return State::BUSY;
}

//
// OpQueryCommand
//
OpQueryCommand::OpQueryCommand(Database* pDatabase,
                               GWBUF* pRequest,
                               packet::Query&& req)
    : PacketCommand<packet::Query>(pDatabase, pRequest, std::move(req), ResponseKind::REPLY)
{
    const auto& query = m_req.query();
    auto it = query.begin();
    auto end = query.end();

    if (it == end)
    {
        m_kind = Kind::EMPTY;
    }
    else
    {
        for (; it != end; ++it)
        {
            auto element = *it;
            auto key = element.key();

            if (key.compare(command::IsMaster::KEY) == 0 || key.compare(key::ISMASTER) == 0)
            {
                m_kind = Kind::IS_MASTER;
                break;
            }
            else if (key.compare(key::QUERY) == 0)
            {
                m_kind = Kind::QUERY;
                break;
            }
            else
            {
                ++it;
            }
        }

        if (it == end)
        {
            m_kind = Kind::IMPLICIT_QUERY;
        }
    }
}

bool OpQueryCommand::session_must_be_ready() const
{
    return m_kind != Kind::IS_MASTER;
}

std::string OpQueryCommand::description() const
{
    return "OP_QUERY";
}

State OpQueryCommand::execute(Response* pNoSQL_response)
{
    State state = State::BUSY;
    GWBUF* pResponse = nullptr;

    switch (m_kind)
    {
    case Kind::EMPTY:
        {
            bsoncxx::document::view query;
            send_query(query);
        }
        break;

    case Kind::IS_MASTER:
        {
            DocumentBuilder doc;
            command::IsMaster::populate_response(m_database, m_req.query(), doc);

            pResponse = create_response(doc.extract());
            state = State::READY;
        }
        break;

    case Kind::QUERY:
        {
            const auto& query = m_req.query();

            send_query(query[key::QUERY].get_document(), query[key::ORDERBY]);
        }
        break;

    case Kind::IMPLICIT_QUERY:
        send_query(m_req.query());
        break;
    }

    pNoSQL_response->reset(pResponse, Response::Status::NOT_CACHEABLE);
    return state;
}

State OpQueryCommand::translate(GWBUF&& mariadb_response, Response* pNoSQL_response)
{
    GWBUF* pResponse = nullptr;

    ComResponse response(mariadb_response.data());

    switch (response.type())
    {
    case ComResponse::ERR_PACKET:
        {
            ComERR err(response);

            auto code = err.code();

            if (code == ER_NO_SUCH_TABLE)
            {
                size_t size_of_documents = 0;
                vector<bsoncxx::document::value> documents;

                pResponse = create_reply_response(0, 0, size_of_documents, documents);
            }
            else
            {
                throw MariaDBError(err);
            }
        }
        break;

    case ComResponse::OK_PACKET:
    case ComResponse::LOCAL_INFILE_PACKET:
        mxb_assert(!true);
        throw_unexpected_packet();
        break;

    default:
        {
            unique_ptr<NoSQLCursor> sCursor = NoSQLCursor::create(table(Quoted::NO),
                                                                  m_extractions,
                                                                  std::move(mariadb_response));

            int32_t position = sCursor->position();
            size_t size_of_documents = 0;
            vector<bsoncxx::document::value> documents;

            sCursor->create_batch(worker(), m_nReturn, m_single_batch, &size_of_documents, &documents);

            int64_t cursor_id = sCursor->exhausted() ? 0 : sCursor->id();

            int32_t response_to = m_request_id;
            int32_t request_id = m_database.context().next_request_id();

            pResponse = create_reply_response(request_id, response_to,
                                              cursor_id, position, size_of_documents, documents);

            // TODO: Somewhat unclear how exhaust should interact with single_batch.
            if (m_req.is_exhaust())
            {
                // Return everything in as many reply packets as needed.
                size_t nReturn = std::numeric_limits<int32_t>::max();

                while (!sCursor->exhausted())
                {
                    position = sCursor->position();

                    documents.clear();
                    sCursor->create_batch(worker(), nReturn, false, &size_of_documents, &documents);

                    cursor_id = sCursor->exhausted() ? 0 : sCursor->id();

                    response_to = request_id;
                    request_id = m_database.context().next_request_id();

                    auto* pMore = create_reply_response(request_id, response_to,
                                                        cursor_id, position, size_of_documents, documents);

                    pResponse->append(*pMore);
                    delete pMore;
                }
            }

            if (!sCursor->exhausted())
            {
                NoSQLCursor::put(std::move(sCursor));
            }
        }
    }

    pNoSQL_response->reset(pResponse, Response::Status::NOT_CACHEABLE);
    return State::READY;
}

void OpQueryCommand::send_query(const bsoncxx::document::view& query,
                                const bsoncxx::document::element& orderby)
{
    ostringstream sql;
    sql << "SELECT ";

    m_extractions = extractions_from_projection(m_req.fields());

    if (!m_extractions.empty())
    {
        string s;
        for (auto extraction : m_extractions)
        {
            if (!s.empty())
            {
                s += ", ";
            }

            s += "JSON_EXTRACT(doc, '$." + extraction + "')";
        }

        sql << s;
    }
    else
    {
        sql << "doc";
    }

    sql << " FROM " << table();

    if (!query.empty())
    {
        sql << where_clause_from_query(query) << " ";
    }

    if (orderby)
    {
        string s = order_by_value_from_sort(orderby.get_document());

        if (!s.empty())
        {
            sql << "ORDER BY " << s << " ";
        }
    }

    sql << "LIMIT ";

    auto nSkip = m_req.nSkip();

    if (m_req.nSkip() != 0)
    {
        sql << nSkip << ", ";
    }

    int64_t nLimit = std::numeric_limits<int64_t>::max();

    if (m_req.nReturn() < 0)
    {
        m_nReturn = -m_req.nReturn();
        nLimit = m_nReturn;
        m_single_batch = true;
    }
    else if (m_req.nReturn() == 1)
    {
        m_nReturn = 1;
        nLimit = m_nReturn;
        m_single_batch = true;
    }
    else if (m_req.nReturn() == 0)
    {
        m_nReturn = DEFAULT_CURSOR_RETURN;
    }
    else
    {
        m_nReturn = m_req.nReturn();
    }

    sql << nLimit;

    send_downstream(sql.str());
}

//
// OpGetMoreCommand
//
string OpGetMoreCommand::description() const
{
    return "OP_GET_MORE";
}

State OpGetMoreCommand::execute(Response* pNoSQL_response)
{
    auto cursor_id = m_req.cursor_id();

    unique_ptr<NoSQLCursor> sCursor = NoSQLCursor::get(m_req.collection(), m_req.cursor_id());

    int32_t position = sCursor->position();
    size_t size_of_documents;
    vector<bsoncxx::document::value> documents;

    sCursor->create_batch(worker(), m_req.nReturn(), false, &size_of_documents, &documents);

    cursor_id = sCursor->exhausted() ? 0 : sCursor->id();

    GWBUF* pResponse = create_reply_response(cursor_id, position, size_of_documents, documents);

    if (!sCursor->exhausted())
    {
        NoSQLCursor::put(std::move(sCursor));
    }

    pNoSQL_response->reset(pResponse, Response::Status::NOT_CACHEABLE);
    return State::READY;
}

State OpGetMoreCommand::translate(GWBUF&& mariadb_response, Response* pNoSQL_response)
{
    mxb_assert(!true);
    return State::READY;
}

//
// OpKillCursorsCommand
//
string OpKillCursorsCommand::description() const
{
    return "OP_KILL_CURSORS";
}

State OpKillCursorsCommand::execute(Response* pNoSQL_response)
{
    NoSQLCursor::kill(m_req.cursor_ids());

    return State::READY;
}

State OpKillCursorsCommand::translate(GWBUF&& mariadb_response, Response* pNoSQL_response)
{
    mxb_assert(!true);
    return State::READY;
}

//
// OpMsgCommand
//
OpMsgCommand::~OpMsgCommand()
{
}

//static
pair<string, OpMsgCommandInfo> OpMsgCommand::get_info(const bsoncxx::document::view& doc)
{
    string name;
    OpMsgCommandInfo info;

    if (!doc.empty())
    {
        // The command *must* be the first element,
        auto element = *doc.begin();
        name.append(element.key().data(), element.key().length());

        auto it = this_unit.infos_by_name.find(mxb::tolower(name));

        if (it != this_unit.infos_by_name.end())
        {
            info = it->second;
        }
    }

    if (!info.create_default)
    {
        name = "unknown";
        info.create_default = &create_default_command<Unknown>;
        info.create_diagnose = &create_diagnose_command<Unknown>;
        info.is_admin = false;
        info.is_cacheable = false;
    }

    return make_pair(name, info);
}

//static
unique_ptr<OpMsgCommand> OpMsgCommand::get(nosql::Database* pDatabase,
                                           GWBUF* pRequest,
                                           packet::Msg&& msg)
{
    auto p = get_info(msg.document());

    const string& name = p.first;
    CreateDefaultFunction create = p.second.create_default;

    return create(name, pDatabase, pRequest, std::move(msg));
}

//static
unique_ptr<OpMsgCommand> OpMsgCommand::get(nosql::Database* pDatabase,
                                           GWBUF* pRequest,
                                           packet::Msg&& msg,
                                           const bsoncxx::document::view& doc,
                                           const DocumentArguments& arguments)
{
    auto p = get_info(doc);

    const string& name = p.first;
    CreateDiagnoseFunction create = p.second.create_diagnose;

    return create(name, pDatabase, pRequest, std::move(msg), doc, arguments);
}

void OpMsgCommand::authenticate()
{
    if (session_must_be_ready() && !m_database.context().authenticated())
    {
        ostringstream ss;
        ss << "command " << m_name << " requires authentication";
        throw SoftError(ss.str(), error::UNAUTHORIZED);
    }
}

GWBUF* OpMsgCommand::create_empty_response() const
{
    auto builder = bsoncxx::builder::stream::document{};
    bsoncxx::document::value doc_value = builder << bsoncxx::builder::stream::finalize;

    return create_response(doc_value);
}

//static
void OpMsgCommand::check_write_batch_size(int size)
{
    if (size < 1 || size > protocol::MAX_WRITE_BATCH_SIZE)
    {
        ostringstream ss;
        ss << "Write batch sizes must be between 1 and " << protocol::MAX_WRITE_BATCH_SIZE
           << ". Got " << size << " operations.";
        throw nosql::SoftError(ss.str(), nosql::error::INVALID_LENGTH);
    }
}

//static
void OpMsgCommand::list_commands(DocumentBuilder& commands)
{
    for (const auto& kv : this_unit.infos_by_name)
    {
        const string& name = kv.first;
        const OpMsgCommandInfo& info = kv.second;

        const char* zHelp = info.zHelp;
        if (!*zHelp)
        {
            zHelp = "no help defined";
        }

        DocumentBuilder command;
        command.append(kvp(key::HELP, zHelp));
        command.append(kvp(key::SLAVE_OK, bsoncxx::types::b_undefined()));
        command.append(kvp(key::ADMIN_ONLY, info.is_admin));
        command.append(kvp(key::REQUIRES_AUTH, (name == "ismaster" ? false : true)));

        // Yes, passing a literal string to kvp as first argument works, but
        // passing a 'const char*' does not.
        commands.append(kvp(string(info.zKey), command.extract()));
    }
}

void OpMsgCommand::require_admin_db()
{
    if (m_database.name() != "admin")
    {
        throw SoftError(m_name + " may only be run against the admin database.",
                        error::UNAUTHORIZED);
    }
}

string OpMsgCommand::convert_skip_and_limit(AcceptAsLimit accept_as_limit) const
{
    string rv;

    auto skip = m_doc[nosql::key::SKIP];
    auto limit = m_doc[nosql::key::LIMIT];

    if (skip || limit)
    {
        int64_t nSkip = 0;
        if (skip && (!get_number_as_integer(skip, &nSkip) || nSkip < 0))
        {
            ostringstream ss;
            int code;
            if (nSkip < 0)
            {
                ss << "Skip value must be non-negative, but received: " << nSkip;
                code = error::BAD_VALUE;
            }
            else
            {
                ss << "Failed to parse: " << bsoncxx::to_json(m_doc) << ". 'skip' field must be numeric.";
                code = error::FAILED_TO_PARSE;
            }

            throw SoftError(ss.str(), code);
        }

        int64_t nLimit = std::numeric_limits<int64_t>::max();
        if (limit)
        {
            if (!get_number_as_integer(limit, &nLimit))
            {
                ostringstream ss;
                ss << "Failed to parse: " << bsoncxx::to_json(m_doc) << ". 'limit' field must be numeric.";
                throw SoftError(ss.str(), error::FAILED_TO_PARSE);
            }

            if (nLimit < 0)
            {
                if (accept_as_limit == AcceptAsLimit::INTEGER)
                {
                    nLimit = -nLimit;
                }
                else
                {
                    ostringstream ss;
                    ss << "Limit value must be non-negative, but received: " << nLimit;
                    throw SoftError(ss.str(), error::BAD_VALUE);
                }
            }
        }

        ostringstream ss;
        ss << "LIMIT ";

        if (nSkip != 0)
        {
            ss << nSkip << ", ";
        }

        if (nLimit == 0)
        {
            // A limit of 0 should have no effect.
            nLimit = std::numeric_limits<int64_t>::max();
        }

        ss << nLimit;

        rv = ss.str();
    }

    return rv;
}

string OpMsgCommand::table(Quoted quoted) const
{
    if (m_quoted_table.empty())
    {
        auto element = m_doc[m_name];
        mxb_assert(element);

        if (element.type() != bsoncxx::type::k_utf8)
        {
            ostringstream ss;
            ss << "collection name has invalid type " << bsoncxx::to_string(element.type());
            throw SoftError(ss.str(), error::BAD_VALUE);
        }

        string_view table = element.get_utf8();

        if (table.length() == 0)
        {
            ostringstream ss;
            ss << "Invalid namespace specified '" << m_database.name() << ".'";
            throw SoftError(ss.str(), error::INVALID_NAMESPACE);
        }

        ostringstream ss1;
        ss1 << "`" << m_database.name() << "`.`" <<  table << "`";

        ostringstream ss2;
        ss2 << m_database.name() << "." << table;

        m_quoted_table = ss1.str();
        m_unquoted_table = ss2.str();
    }

    return quoted == Quoted::YES ? m_quoted_table : m_unquoted_table;
}

void OpMsgCommand::add_error(bsoncxx::builder::basic::array& array, const ComERR& err, int index)
{
    bsoncxx::builder::basic::document mariadb;

    mariadb.append(bsoncxx::builder::basic::kvp(key::INDEX, index));
    mariadb.append(bsoncxx::builder::basic::kvp(key::CODE, err.code()));
    mariadb.append(bsoncxx::builder::basic::kvp(key::STATE, err.state()));
    mariadb.append(bsoncxx::builder::basic::kvp(key::MESSAGE, err.message()));

    // TODO: Map MariaDB errors to something sensible from
    // TODO: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml

    bsoncxx::builder::basic::document error;

    interpret_error(error, err, index);
    error.append(bsoncxx::builder::basic::kvp(key::MARIADB, mariadb.extract()));

    array.append(error.extract());
}

void OpMsgCommand::add_error(bsoncxx::builder::basic::document& response, const ComERR& err)
{
    bsoncxx::builder::basic::array array;

    add_error(array, err, 0);

    response.append(bsoncxx::builder::basic::kvp(key::WRITE_ERRORS, array.extract()));
}

void OpMsgCommand::interpret_error(bsoncxx::builder::basic::document& error, const ComERR& err, int index)
{
    auto code = error::from_mariadb_code(err.code());
    auto errmsg = err.message();

    error.append(bsoncxx::builder::basic::kvp(key::INDEX, index));
    error.append(bsoncxx::builder::basic::kvp(key::CODE, code));
    error.append(bsoncxx::builder::basic::kvp(key::ERRMSG, errmsg));

    m_database.context().set_last_error(std::make_unique<ConcreteLastError>(errmsg, code));
}

State ImmediateCommand::execute(Response* pNoSQL_response)
{
    DocumentBuilder doc;
    auto status = populate_response(doc);

    pNoSQL_response->reset(create_response(doc.extract()), status);
    return State::READY;
}

State ImmediateCommand::translate(GWBUF&& mariadb_response, Response* pNoSQL_response)
{
    // This will never be called.
    mxb_assert(!true);
    throw std::runtime_error("ImmediateCommand::translate(...) should not be called.");
    return State::READY;
}

void ImmediateCommand::diagnose(DocumentBuilder& doc)
{
    doc.append(kvp(key::KIND, value::IMMEDIATE));

    DocumentBuilder response;
    populate_response(response);

    doc.append(kvp(key::RESPONSE, response.extract()));
}

State SingleCommand::execute(Response* pNoSQL_response)
{
    prepare();

    string statement = generate_sql();

    m_statement = std::move(statement);

    send_downstream(m_statement);

    return State::BUSY;
}

void SingleCommand::prepare()
{
}

void SingleCommand::diagnose(DocumentBuilder& doc)
{
    doc.append(kvp(key::KIND, value::SINGLE));
    doc.append(kvp(key::SQL, generate_sql()));
}

void MultiCommand::diagnose(DocumentBuilder& doc)
{
    doc.append(kvp(key::KIND, value::MULTI));
    const auto& query = generate_sql();

    ArrayBuilder sql;
    for (const auto& statement : query.statements())
    {
        sql.append(statement);
    }

    doc.append(kvp(key::SQL, sql.extract()));
}

}
