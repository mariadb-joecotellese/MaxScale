/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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

#include "nosqlcursor.hh"
#include <sstream>
#include <maxbase/worker.hh>
#include <maxscale/mainworker.hh>
#include "nosqlcommand.hh"

using std::set;
using std::string;
using std::ostringstream;
using std::vector;

namespace
{

using namespace nosql;

class ThisUnit : public mxb::Worker::Callable
{
public:
    ThisUnit()
        : mxb::Worker::Callable(mxs::MainWorker::get())
    {
    }

    ThisUnit(const ThisUnit&) = delete;
    ThisUnit& operator=(const ThisUnit&) = delete;

    int64_t next_id()
    {
        // TODO: Later we probably want to create a random id, not a guessable one.
        return ++m_id;
    }

    void put_cursor(std::unique_ptr<NoSQLCursor> sCursor)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        CursorsById& cursors = m_collection_cursors[sCursor->ns()];

        mxb_assert(cursors.find(sCursor->id()) == cursors.end());

        cursors.insert(std::make_pair(sCursor->id(), std::move(sCursor)));
    }

    std::unique_ptr<NoSQLCursor> get_cursor(const std::string& collection, int64_t id)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        auto it = m_collection_cursors.find(collection);

        if (it == m_collection_cursors.end())
        {
            throw_cursor_not_found(id);
        }

        CursorsById& cursors = it->second;

        auto jt = cursors.find(id);

        if (jt == cursors.end())
        {
            throw_cursor_not_found(id);
        }

        auto sCursor = std::move(jt->second);

        cursors.erase(jt);

        if (cursors.size() == 0)
        {
            m_collection_cursors.erase(it);
        }

        return sCursor;
    }

    std::set<int64_t> kill_cursors(const std::string& collection, const std::vector<int64_t>& ids)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        set<int64_t> removed;

        auto it = m_collection_cursors.find(collection);

        if (it != m_collection_cursors.end())
        {
            CursorsById& cursors = it->second;

            for (auto id : ids)
            {
                auto jt = cursors.find(id);

                if (jt != cursors.end())
                {
                    cursors.erase(jt);
                    removed.insert(id);
                }
            }
        }

        return removed;
    }

    std::set<int64_t> kill_cursors(const std::vector<int64_t>& ids)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        set<int64_t> removed;

        for (auto id : ids)
        {
            for (auto& kv : m_collection_cursors)
            {
                CursorsById& cursors = kv.second;

                auto it = cursors.find(id);

                if (it != cursors.end())
                {
                    cursors.erase(it);
                    removed.insert(id);
                    break;
                }
            }
        }

        return removed;
    }

    void kill_idle_cursors(const mxb::TimePoint& now, const std::chrono::seconds& timeout)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        for (auto& kv : m_collection_cursors)
        {
            CursorsById& cursors = kv.second;

            auto it = cursors.begin();

            while (it != cursors.end())
            {
                auto& sCursor = it->second;

                auto idle = now - sCursor->last_use();

                if (idle > timeout)
                {
                    it = cursors.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }
    }

    void purge(const std::string& collection)
    {
        std::lock_guard<std::mutex> guard(m_mutex);

        auto it = m_collection_cursors.find(collection);

        if (it != m_collection_cursors.end())
        {
            m_collection_cursors.erase(it);
        }
    }

private:
    void throw_cursor_not_found(int64_t id)
    {
        ostringstream ss;
        ss << "cursor id " << id << " not found";
        throw nosql::SoftError(ss.str(), nosql::error::CURSOR_NOT_FOUND);
    }

    using CursorsById = std::unordered_map<int64_t, std::unique_ptr<NoSQLCursor>>;
    using CollectionCursors = std::unordered_map<std::string, CursorsById>;

    std::atomic<int64_t> m_id;
    std::mutex           m_mutex;
    CollectionCursors    m_collection_cursors;
} this_unit;

// If bit 63 is 0 and bit 62 a 1, then the value is interpreted as a 'Long'.
const int64_t BSON_LONG_BIT = (int64_t(1) << 62);

}

namespace nosql
{

NoSQLCursor::NoSQLCursor(const std::string& ns)
    : m_ns(ns)
    , m_id(0)
{
}

NoSQLCursor::NoSQLCursor(const std::string& ns,
                         const vector<string>& extractions,
                         GWBUF&& mariadb_response)
    : m_ns(ns)
    , m_id(this_unit.next_id() | BSON_LONG_BIT)
    , m_extractions(std::move(extractions))
    , m_mariadb_response(std::move(mariadb_response))
    , m_pBuffer(m_mariadb_response.data())
    , m_nBuffer(m_mariadb_response.length())
{
    initialize();
}

//static
std::unique_ptr<NoSQLCursor> NoSQLCursor::create(const std::string& ns)
{
    return std::unique_ptr<NoSQLCursor>(new NoSQLCursor(ns));
}

//static
std::unique_ptr<NoSQLCursor> NoSQLCursor::create(const std::string& ns,
                                                 const std::vector<std::string>& extractions,
                                                 GWBUF&& mariadb_response)
{
    return std::unique_ptr<NoSQLCursor>(new NoSQLCursor(ns, extractions, std::move(mariadb_response)));
}

//static
std::unique_ptr<NoSQLCursor> NoSQLCursor::get(const std::string& collection, int64_t id)
{
    return this_unit.get_cursor(collection, id);
}

//static
void NoSQLCursor::put(std::unique_ptr<NoSQLCursor> sCursor)
{
    this_unit.put_cursor(std::move(sCursor));
}

//static
std::set<int64_t> NoSQLCursor::kill(const std::string& collection, const std::vector<int64_t>& ids)
{
    return this_unit.kill_cursors(collection, ids);
}

//static
std::set<int64_t> NoSQLCursor::kill(const std::vector<int64_t>& ids)
{
    return this_unit.kill_cursors(ids);
}

//static
void NoSQLCursor::kill_idle(const mxb::TimePoint& now, const std::chrono::seconds& timeout)
{
    this_unit.kill_idle_cursors(now, timeout);
}

//static
void NoSQLCursor::start_purging_idle_cursors(const std::chrono::seconds& cursor_timeout)
{
    // This should be called at startup, so we must be on MainWorker.
    mxb_assert(mxs::MainWorker::is_current());

    auto* pMain = mxs::MainWorker::get();

    // The time between checks whether cursors need to be killed is defined
    // as 1/10 of the cursor timeout, but at least 1 second.
    std::chrono::milliseconds wait_timeout = cursor_timeout;
    wait_timeout /= 10;

    if (wait_timeout == std::chrono::milliseconds(0))
    {
        wait_timeout = std::chrono::milliseconds(1000);
    }

    // We don't ever want to cancel this explicitly so the delayed call will
    // be cancelled when MainWorker is destructed.
    this_unit.dcall(wait_timeout, [pMain, cursor_timeout]() {
            kill_idle(pMain->epoll_tick_now(), cursor_timeout);

            return true; // Call again
        });
}

void NoSQLCursor::create_first_batch(mxb::Worker& worker,
                                     bsoncxx::builder::basic::document& doc,
                                     int32_t nBatch,
                                     bool single_batch)
{
    create_batch(worker, doc, key::FIRST_BATCH, nBatch, single_batch);
}

void NoSQLCursor::create_next_batch(mxb::Worker& worker,
                                    bsoncxx::builder::basic::document& doc, int32_t nBatch)
{
    create_batch(worker, doc, key::NEXT_BATCH, nBatch, false);
}

//static
void NoSQLCursor::create_first_batch(bsoncxx::builder::basic::document& doc,
                                     const std::string& ns)
{
    ArrayBuilder batch;

    int64_t id = 0;

    DocumentBuilder cursor;
    cursor.append(kvp(key::FIRST_BATCH, batch.extract()));
    cursor.append(kvp(key::ID, id));
    cursor.append(kvp(key::NS, ns));

    doc.append(kvp(key::CURSOR, cursor.extract()));
    doc.append(kvp(key::OK, 1));
}

//static
void NoSQLCursor::purge(const std::string& collection)
{
    this_unit.purge(collection);
}

void NoSQLCursor::create_batch(mxb::Worker& worker,
                               int32_t nBatch,
                               bool single_batch,
                               size_t* pSize_of_documents,
                               std::vector<bsoncxx::document::value>* pDocuments)
{
    mxb_assert(!m_exhausted);

    size_t size_of_documents = 0;
    vector<bsoncxx::document::value> documents;

    if (m_pBuffer)
    {
        create_batch([&size_of_documents, &documents](bsoncxx::document::value&& doc)
                     {
                         size_t size = doc.view().length();

                         if (size_of_documents + size > protocol::MAX_MSG_SIZE)
                         {
                             return false;
                         }
                         else
                         {
                             size_of_documents += size;
                             documents.emplace_back(std::move(doc));
                             return true;
                         }
                     },
                     nBatch);
    }
    else
    {
        m_exhausted = true;
    }

    if (single_batch)
    {
        m_exhausted = true;
    }

    *pSize_of_documents = size_of_documents;
    pDocuments->swap(documents);

    touch(worker);
}

void NoSQLCursor::create_batch(mxb::Worker& worker,
                               bsoncxx::builder::basic::document& doc,
                               const string& which_batch,
                               int32_t nBatch,
                               bool single_batch)
{
    mxb_assert(!m_exhausted);

    ArrayBuilder batch;
    size_t total_size = 0;

    int64_t id = 0;

    if (m_pBuffer)
    {
        if (create_batch([&batch, &total_size](bsoncxx::document::value&& document)
                         {
                             size_t size = document.view().length();

                             if (total_size + size > protocol::MAX_BSON_OBJECT_SIZE)
                             {
                                 return false;
                             }
                             else
                             {
                                 total_size += size;

                                 batch.append(document);
                                 return true;
                             }
                         },
                         nBatch) == Result::PARTIAL)
        {
            id = m_id;
        }
    }
    else
    {
        m_exhausted = true;
    }

    if (single_batch)
    {
        m_exhausted = true;
        id = 0;
    }

    DocumentBuilder cursor;
    cursor.append(kvp(which_batch, batch.extract()));
    cursor.append(kvp(key::ID, id));
    cursor.append(kvp(key::NS, m_ns));

    doc.append(kvp(key::CURSOR, cursor.extract()));
    doc.append(kvp(key::OK, 1));

    touch(worker);
}

NoSQLCursor::Result NoSQLCursor::create_batch(std::function<bool(bsoncxx::document::value&& doc)> append,
                                              int32_t nBatch)
{
    int n = 0;

    while (n < nBatch && ComResponse(m_pBuffer, m_nBuffer).type() != ComResponse::EOF_PACKET)
    {
         // m_pBuffer was not advanced above.
        ++n;
        // m_pBuffer cannot be advanced before we know whether the object will fit.
        auto pBuffer = m_pBuffer;
        size_t nBuffer = m_nBuffer;
        CQRTextResultsetRow row(&pBuffer, &nBuffer, m_types); // Advances pBuffer

        string json = resultset_row_to_json(row, m_extractions);

        auto doc = nosql::bson_from_json(json);

        if (!append(std::move(doc)))
        {
            // TODO: Don't discard the converted doc, but store it somewhere for
            // TODO: the next batch.
            break;
        }

        m_nBuffer = nBuffer;
        m_pBuffer = pBuffer;
    }

    bool at_end = (ComResponse(m_pBuffer).type() == ComResponse::EOF_PACKET);

    if (at_end)
    {
        ComResponse response(&m_pBuffer);
        m_exhausted = true;
    }

    m_position += n;

    return at_end ? Result::COMPLETE : Result::PARTIAL;
}

void NoSQLCursor::initialize()
{
    ComQueryResponse cqr(&m_pBuffer);

    auto nFields = cqr.nFields();

    // If there are no extractions, then we SELECTed the entire document and there should
    // be just one field (the JSON document). Otherwise there should be as many fields
    // (JSON_EXTRACT(doc, '$...')) as there are extractions.
    mxb_assert((m_extractions.empty() && nFields == 1) || (m_extractions.size() == nFields));

    for (size_t i = 0; i < nFields; ++i)
    {
        // ... and then as many column definitions.
        ComQueryResponse::ColumnDef column_def(&m_pBuffer);

        m_names.push_back(column_def.name().to_string());
        m_types.push_back(column_def.type());
    }

    // The there should be an EOF packet, which should be bypassed.
    ComResponse eof(&m_pBuffer);
    mxb_assert(eof.type() == ComResponse::EOF_PACKET);

    // Now m_pBuffer points at the beginning of rows.
}

void NoSQLCursor::touch(mxb::Worker& worker)
{
    m_used = worker.epoll_tick_now();
}

int32_t NoSQLCursor::nRemaining() const
{
    int32_t n = 0;

    auto pBuffer = m_pBuffer;
    auto nBuffer = m_nBuffer;

    if (pBuffer != pBuffer + nBuffer)
    {
        while (ComResponse(pBuffer, nBuffer).type() != ComResponse::EOF_PACKET)
        {
            ++n;
            CQRTextResultsetRow row(&pBuffer, &nBuffer, m_types); // Advances pBuffer
        }
    }

    return n;
}

}
