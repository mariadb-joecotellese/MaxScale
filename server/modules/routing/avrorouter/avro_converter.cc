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

#include "avro_converter.hh"

#include <limits.h>
#include <sys/stat.h>
#include <glob.h>
#include <unistd.h>

#include <tuple>

#include <maxbase/assert.hh>

namespace
{

int rowevent_to_enum_offset(RowEvent event)
{
    switch (event)
    {
    case RowEvent::WRITE:
        return 0;

    case RowEvent::UPDATE:
        return 1;

    case RowEvent::UPDATE_AFTER:
        return 2;

    case RowEvent::DELETE:
        return 3;

    default:
        mxb_assert(!true);
        return 0;
    }
}

std::pair<bool, int64_t> get_file_size(const char* filename)
{
    bool ok = false;
    int64_t size = 0;

    if (struct stat st; stat(filename, &st) == 0)
    {
        size = st.st_size;
        ok = true;
    }
    else
    {
        MXB_INFO("Call to stat() failed for '%s': %d, %s", filename, errno, mxb_strerror(errno));
    }

    return {ok, size};
}
}

/**
 * @brief Allocate an Avro table
 *
 * Create an Aro table and prepare it for writing.
 * @param filepath Path to the created file
 * @param json_schema The schema of the table in JSON format
 */
AvroTable* avro_table_alloc(const char* filepath,
                            const char* json_schema,
                            const char* codec,
                            size_t block_size)
{
    avro_file_writer_t avro_file;
    avro_value_iface_t* avro_writer_iface;
    avro_schema_t avro_schema;

    if (avro_schema_from_json_length(json_schema,
                                     strlen(json_schema),
                                     &avro_schema))
    {
        MXB_ERROR("Avro error: %s", avro_strerror());
        MXB_INFO("Avro schema: %s", json_schema);
        return NULL;
    }

    int rc = 0;

    if (access(filepath, F_OK) == 0)
    {
        rc = avro_file_writer_open_bs(filepath, &avro_file, block_size);
    }
    else
    {
        rc = avro_file_writer_create_with_codec(filepath,
                                                avro_schema,
                                                &avro_file,
                                                codec,
                                                block_size);
    }

    if (rc)
    {
        MXB_ERROR("Avro error: %s", avro_strerror());
        avro_schema_decref(avro_schema);
        return NULL;
    }

    if ((avro_writer_iface = avro_generic_class_from_schema(avro_schema)) == NULL)
    {
        MXB_ERROR("Avro error: %s", avro_strerror());
        avro_schema_decref(avro_schema);
        avro_file_writer_close(avro_file);
        return NULL;
    }

    AvroTable* table = new(std::nothrow) AvroTable(avro_file, avro_writer_iface, avro_schema, filepath);

    if (!table)
    {
        avro_file_writer_close(avro_file);
        avro_value_iface_decref(avro_writer_iface);
        avro_schema_decref(avro_schema);
        MXB_OOM();
    }

    return table;
}

static const char* codec_to_string(enum mxs_avro_codec_type type)
{
    switch (type)
    {
    case MXS_AVRO_CODEC_NULL:
        return "null";

    case MXS_AVRO_CODEC_DEFLATE:
        return "deflate";

    case MXS_AVRO_CODEC_SNAPPY:
        return "snappy";

    default:
        mxb_assert(false);
        return "null";
    }
}

AvroConverter::AvroConverter(SERVICE* service,
                             std::string avrodir,
                             uint64_t block_size,
                             mxs_avro_codec_type codec,
                             int64_t max_size,
                             int64_t max_age)
    : m_avrodir(avrodir)
    , m_block_size(block_size)
    , m_codec(codec)
    , m_service(service)
    , m_max_size(max_size)
    , m_max_age(max_age)
{
}

bool AvroConverter::create_table(const Table& create)
{
    return true;
}

bool AvroConverter::open_table(const Table& create)
{
    bool rval = false;

    if (json_t* json = create.to_json())
    {
        std::string json_schema = mxb::json_dump(json);
        json_decref(json);

        char filepath[PATH_MAX + 1];
        snprintf(filepath,
                 sizeof(filepath),
                 "%s/%s.%s.%06d.avro",
                 m_avrodir.c_str(),
                 create.database.c_str(),
                 create.table.c_str(),
                 create.version);

        SAvroTable avro_table(avro_table_alloc(filepath,
                                               json_schema.c_str(),
                                               codec_to_string(m_codec),
                                               m_block_size));

        if (avro_table)
        {
            m_open_tables[create.id()] = avro_table;
            rval = true;

            if (m_max_age > 0)
            {
                purge_old_files(create.id());
            }
        }
        else
        {
            MXB_ERROR("Failed to open new Avro file for writing.");
        }
    }
    else
    {
        MXB_ERROR("Failed to create JSON schema.");
    }

    return rval;
}

bool AvroConverter::prepare_table(const Table& create)
{
    bool rval = false;
    auto it = m_open_tables.find(create.id());

    if (it != m_open_tables.end())
    {
        m_writer_iface = it->second->avro_writer_iface;
        m_avro_file = &it->second->avro_file;
        rval = true;
    }

    return rval;
}

void AvroConverter::flush_tables()
{
    for (auto it = m_open_tables.begin(); it != m_open_tables.end(); it++)
    {
        avro_file_writer_flush(it->second->avro_file);
    }

    AvroSession::notify_all_clients(m_service);
}

void AvroConverter::prepare_row(const Table& create, const gtid_pos_t& gtid,
                                const REP_HEADER& hdr, RowEvent event_type)
{
    avro_generic_value_new(m_writer_iface, &m_record);
    avro_value_get_by_name(&m_record, avro_domain, &m_field, NULL);
    avro_value_set_int(&m_field, gtid.domain);

    avro_value_get_by_name(&m_record, avro_server_id, &m_field, NULL);
    avro_value_set_int(&m_field, gtid.server_id);

    avro_value_get_by_name(&m_record, avro_sequence, &m_field, NULL);
    avro_value_set_int(&m_field, gtid.seq);

    avro_value_get_by_name(&m_record, avro_event_number, &m_field, NULL);
    avro_value_set_int(&m_field, gtid.event_num);

    avro_value_get_by_name(&m_record, avro_timestamp, &m_field, NULL);
    avro_value_set_int(&m_field, hdr.timestamp);

    avro_value_get_by_name(&m_record, avro_event_type, &m_field, NULL);
    avro_value_set_enum(&m_field, rowevent_to_enum_offset(event_type));
}

bool AvroConverter::commit(const Table& create, const gtid_pos_t& gtid)
{
    bool rval = true;

    if (avro_file_writer_append_value(*m_avro_file, &m_record))
    {
        MXB_ERROR("Failed to write value: %s", avro_strerror());
        rval = false;
    }

    avro_value_decref(&m_record);

    return rval;
}

void AvroConverter::column_int(const Table& create, int i, int32_t value)
{
    set_active(create, i);
    avro_value_set_int(&m_field, value);
}

void AvroConverter::column_long(const Table& create, int i, int64_t value)
{
    set_active(create, i);

    if (avro_value_get_type(&m_field) == AVRO_INT32)
    {
        // Pre-2.4.3 versions use int for 32-bit integers whereas 2.4.3 and newer use long
        avro_value_set_int(&m_field, value);
    }
    else
    {
        avro_value_set_long(&m_field, value);
    }
}

void AvroConverter::column_float(const Table& create, int i, float value)
{
    set_active(create, i);
    avro_value_set_float(&m_field, value);
}

void AvroConverter::column_double(const Table& create, int i, double value)
{
    set_active(create, i);
    avro_value_set_double(&m_field, value);
}

void AvroConverter::column_string(const Table& create, int i, const std::string& value)
{
    set_active(create, i);
    avro_value_set_string(&m_field, value.c_str());
}

void AvroConverter::column_bytes(const Table& create, int i, uint8_t* value, int len)
{
    set_active(create, i);
    avro_value_set_bytes(&m_field, value, len);
}

void AvroConverter::column_null(const Table& create, int i)
{
    set_active(create, i);
    avro_value_set_branch(&m_union_value, 0, &m_field);
    avro_value_set_null(&m_field);
}

void AvroConverter::set_active(const Table& create, int i)
{
    MXB_AT_DEBUG(int rc = ) avro_value_get_by_name(&m_record,
                                                   create.columns[i].name.c_str(),
                                                   &m_union_value,
                                                   NULL);
    mxb_assert(rc == 0);
    avro_value_set_branch(&m_union_value, 1, &m_field);
}

bool AvroConverter::needs_rotate(const Table& create) const
{
    bool rval = false;

    if (m_max_size > 0)
    {
        if (auto it = m_open_tables.find(create.id()); it != m_open_tables.end())
        {
            if (auto [ok, size] = get_file_size(it->second->filename.c_str()); ok)
            {
                rval = size >= m_max_size;
            }
        }
    }

    return rval;
}

void AvroConverter::purge_old_files(std::string id)
{
    std::string path = m_avrodir + "/" + id + ".*.avro";
    glob_t files;

    if (glob(path.c_str(), 0, NULL, &files) == 0)
    {
        // The files are sorted and the last one is always the newest one. No point in checking it since it
        // was just created.
        for (size_t i = 0; i < files.gl_pathc - 1; i++)
        {
            int64_t newest = 0;
            time_t now = time(nullptr);

            if (MAXAVRO_FILE* file = maxavro_file_open(files.gl_pathv[i]))
            {
                maxavro_to_last_block(file);

                // Check the timestamps from the last block, helps avoid processing the whole file.
                while (json_t* rec = maxavro_record_read_json(file))
                {
                    newest = mxb::Json(rec, mxb::Json::RefType::STEAL).get_int("timestamp");
                }

                maxavro_file_close(file);
            }

            if (int64_t age = now - newest; newest > 0 && age > m_max_age)
            {
                std::string schema(files.gl_pathv[i]);
                schema.replace(schema.length() - 2, 2, "sc");

                if (mxb_log_should_log(LOG_INFO))
                {
                    auto bytes = get_file_size(files.gl_pathv[i]).second
                        + get_file_size(schema.c_str()).second;

                    MXB_INFO("Removing files '%s' and '%s' (%ld bytes in total), data is %ld seconds old",
                             files.gl_pathv[i], schema.c_str(), bytes, age);
                }

                if (unlink(files.gl_pathv[i]) != 0)
                {
                    MXB_ERROR("Failed to remove data file '%s': %d, %s",
                              files.gl_pathv[i], errno, mxb_strerror(errno));
                }
                else if (unlink(schema.c_str()) != 0)
                {
                    MXB_ERROR("Failed to remove schema file '%s': %d, %s",
                              schema.c_str(), errno, mxb_strerror(errno));
                }
            }
        }
    }

    globfree(&files);
}
