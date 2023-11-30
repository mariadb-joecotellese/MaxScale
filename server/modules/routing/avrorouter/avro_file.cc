/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file avro_file.c - Legacy file operations for the Avro router
 */

#include "avrorouter.hh"

#include <blr_constants.hh>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#include <maxbase/ini.hh>
#include <maxscale/maxscale.hh>
#include <maxscale/pcre2.hh>
#include <maxscale/routingworker.hh>

static const char* statefile_section = "avro-conversion";


/**
 * Open a binlog file for reading
 *
 * @param router    The router instance
 * @param file      The binlog file name
 */
bool avro_open_binlog(const char* binlogdir, const char* file, int* dest)
{
    char path[PATH_MAX + 1] = "";
    int fd;

    snprintf(path, sizeof(path), "%s/%s", binlogdir, file);

    if ((fd = open(path, O_RDONLY)) == -1)
    {
        if (errno != ENOENT)
        {
            MXB_ERROR("Failed to open binlog file %s: %d, %s",
                      path,
                      errno,
                      mxb_strerror(errno));
        }
        return false;
    }

    if (lseek(fd, BINLOG_MAGIC_SIZE, SEEK_SET) < 4)
    {
        /* If for any reason the file's length is between 1 and 3 bytes
         * then report an error. */
        MXB_ERROR("Binlog file %s has an invalid length.", path);
        close(fd);
        return false;
    }

    *dest = fd;
    return true;
}

/**
 * @brief Write a new ini file with current conversion status
 *
 * The file is stored in the cache directory as 'avro-conversion.ini'.
 * @param router Avro router instance
 * @return True if the file was written successfully to disk
 *
 */
bool avro_save_conversion_state(Avro* router)
{
    FILE* config_file;
    char filename[PATH_MAX + 1];

    snprintf(filename, sizeof(filename), "%s/" AVRO_PROGRESS_FILE ".tmp", router->config().avrodir.c_str());

    /* open file for writing */
    config_file = fopen(filename, "wb");

    if (config_file == NULL)
    {
        MXB_ERROR("Failed to open file '%s': %d, %s",
                  filename,
                  errno,
                  mxb_strerror(errno));
        return false;
    }

    gtid_pos_t gtid = router->handler->get_gtid();
    fprintf(config_file, "[%s]\n", statefile_section);
    fprintf(config_file, "position=%lu\n", router->current_pos);
    fprintf(config_file,
            "gtid=%lu-%lu-%lu:%lu\n",
            gtid.domain,
            gtid.server_id,
            gtid.seq,
            gtid.event_num);
    fprintf(config_file, "file=%s\n", router->binlog_name.c_str());
    fclose(config_file);

    /* rename tmp file to right filename */
    char newname[PATH_MAX + 1];
    snprintf(newname, sizeof(newname), "%s/" AVRO_PROGRESS_FILE, router->config().avrodir.c_str());
    int rc = rename(filename, newname);

    if (rc == -1)
    {
        MXB_ERROR("Failed to rename file '%s' to '%s': %d, %s",
                  filename,
                  newname,
                  errno,
                  mxb_strerror(errno));
        return false;
    }

    return true;
}

/**
 * @brief Callback for the @c ini_parse of the stored conversion position
 *
 * @param data User provided data
 * @param section Section name
 * @param key Parameter name
 * @param value Parameter value
 * @return 1 if the parsing should continue, 0 if an error was detected
 */
static int conv_state_handler(void* data, const char* section, const char* key, const char* value, int lineno)
{
    if (!key && !value)
    {
        // Ignore section name updates.
        return 1;
    }

    Avro* router = (Avro*)data;

    if (strcmp(section, statefile_section) == 0)
    {
        if (strcmp(key, "gtid") == 0)
        {
            gtid_pos_t gtid;
            MXB_AT_DEBUG(bool rval = ) gtid.parse(value);
            mxb_assert(rval);
            router->handler->set_gtid(gtid);
        }
        else if (strcmp(key, "position") == 0)
        {
            router->current_pos = strtol(value, NULL, 10);
        }
        else if (strcmp(key, "file") == 0)
        {
            size_t len = strlen(value);

            if (len > BINLOG_FNAMELEN)
            {
                MXB_ERROR("Provided value %s for key 'file' is too long. "
                          "The maximum allowed length is %d.",
                          value,
                          BINLOG_FNAMELEN);
                return 0;
            }

            router->binlog_name = value;
        }
        else
        {
            return 0;
        }
    }

    return 1;
}

/**
 * @brief Load a stored conversion state from file
 *
 * @param router Avro router instance
 * @return True if the stored state was loaded successfully
 */
bool avro_load_conversion_state(Avro* router)
{
    char filename[PATH_MAX + 1];
    bool rval = false;

    snprintf(filename, sizeof(filename), "%s/" AVRO_PROGRESS_FILE, router->config().avrodir.c_str());

    /** No stored state, this is the first time the router is started */
    if (access(filename, F_OK) == -1)
    {
        return true;
    }

    MXB_NOTICE("[%s] Loading stored conversion state: %s", router->service->name(), filename);

    int rc = mxb::ini::parse_file(filename, conv_state_handler, router);

    switch (rc)
    {
    case 0:
        {
            rval = true;
            gtid_pos_t gtid = router->handler->get_gtid();
            MXB_NOTICE(
                "Loaded stored binary log conversion state: File: [%s] Position: [%ld] GTID: [%lu-%lu-%lu:%lu]",
                router->binlog_name.c_str(),
                router->current_pos,
                gtid.domain,
                gtid.server_id,
                gtid.seq,
                gtid.event_num);
        }
        break;

    case -1:
        MXB_ERROR("Failed to open file '%s'. ", filename);
        break;

    case -2:
        MXB_ERROR("Failed to allocate enough memory when parsing file '%s'. ", filename);
        break;

    default:
        MXB_ERROR("Failed to parse stored conversion state '%s', error "
                  "on line %d. ",
                  filename,
                  rc);
        break;
    }

    return rval;
}

/**
 * Get the next binlog file name.
 *
 * @param router    The router instance
 * @return 0 on error, >0 as sequence number
 */
int get_next_binlog(const char* binlog_name)
{
    const char* sptr;
    int filenum;

    if ((sptr = strrchr(binlog_name, '.')) == NULL)
    {
        return 0;
    }
    filenum = atoi(sptr + 1);
    if (filenum)
    {
        filenum++;
    }

    return filenum;
}

/**
 * @brief Check if the next binlog file exists and is readable
 * @param binlogdir Directory where the binlogs are
 * @param binlog Current binlog name
 * @return True if next binlog file exists and is readable
 */
bool binlog_next_file_exists(const char* binlogdir, const char* binlog)
{
    bool rval = false;
    int filenum = get_next_binlog(binlog);

    if (filenum)
    {
        const char* sptr = strrchr(binlog, '.');

        if (sptr)
        {
            char buf[BLRM_BINLOG_NAME_STR_LEN + 1];
            char filename[PATH_MAX + 1];
            char next_file[BLRM_BINLOG_NAME_STR_LEN + 1 + 20];
            int offset = sptr - binlog;
            memcpy(buf, binlog, offset);
            buf[offset] = '\0';
            snprintf(next_file, sizeof(next_file), BINLOG_NAMEFMT, buf, filenum);
            snprintf(filename, PATH_MAX, "%s/%s", binlogdir, next_file);
            filename[PATH_MAX] = '\0';

            /* Next file in sequence doesn't exist */
            if (access(filename, R_OK) == -1)
            {
                MXB_DEBUG("File '%s' does not yet exist.", filename);
            }
            else
            {
                rval = true;
            }
        }
    }

    return rval;
}

/**
 * @brief Rotate to next file if it exists
 *
 * @param router Avro router instance
 * @param pos Current position, used for logging
 * @param stop_seen If a stop event was seen when processing current file
 * @return AVRO_OK if the next file exists, AVRO_LAST_FILE if this is the last
 * available file.
 */
static avro_binlog_end_t rotate_to_next_file_if_exists(Avro* router, uint64_t pos)
{
    avro_binlog_end_t rval = AVRO_LAST_FILE;

    if (binlog_next_file_exists(router->config().binlogdir.c_str(), router->binlog_name.c_str()))
    {
        char next_binlog[BINLOG_FNAMELEN + 1];
        if (snprintf(next_binlog,
                     sizeof(next_binlog),
                     BINLOG_NAMEFMT,
                     router->config().filestem.c_str(),
                     get_next_binlog(router->binlog_name.c_str())) >= (int)sizeof(next_binlog))
        {
            MXB_ERROR("Next binlog name did not fit into the allocated buffer "
                      "but was truncated, aborting: %s",
                      next_binlog);
            rval = AVRO_BINLOG_ERROR;
        }
        else
        {
            MXB_INFO("End of binlog file [%s] at %lu. Rotating to next binlog file [%s].",
                     router->binlog_name.c_str(),
                     pos,
                     next_binlog);
            rval = AVRO_OK;
            router->binlog_name = next_binlog;
            router->current_pos = 4;
        }
    }

    return rval;
}

/**
 * @brief Rotate to a specific file
 *
 * This rotates the current binlog file being processed to a specific file.
 * Currently this is only used to rotate to files that rotate events point to.
 * @param router Avro router instance
 * @param pos Current position, only used for logging
 * @param next_binlog The next file to rotate to
 */
static void rotate_to_file(Avro* router, uint64_t pos, const char* next_binlog)
{
    MXB_NOTICE("End of binlog file [%s] at %lu. Rotating to file [%s].",
               router->binlog_name.c_str(),
               pos,
               next_binlog);
    router->binlog_name = next_binlog;
    router->current_pos = 4;
}

/**
 * @brief Read the replication event payload
 *
 * @param router Avro router instance
 * @param hdr Replication header
 * @param pos Starting position of the event header
 * @return The event data or NULL if an error occurred
 */
static GWBUF read_event_data(Avro* router, REP_HEADER* hdr, uint64_t pos)
{
    GWBUF result(hdr->event_size - BINLOG_EVENT_HDR_LEN + 1);

    uint8_t* data = result.data();
    int n = pread(router->binlog_fd,
                  data,
                  hdr->event_size - BINLOG_EVENT_HDR_LEN,
                  pos + BINLOG_EVENT_HDR_LEN);
    /** NULL-terminate for QUERY_EVENT processing */
    data[hdr->event_size - BINLOG_EVENT_HDR_LEN] = '\0';

    if (n != static_cast<int>(hdr->event_size - BINLOG_EVENT_HDR_LEN))
    {
        if (n == -1)
        {
            MXB_ERROR("Error reading the event at %lu in %s. "
                      "%s, expected %d bytes.",
                      pos,
                      router->binlog_name.c_str(),
                      mxb_strerror(errno),
                      hdr->event_size - BINLOG_EVENT_HDR_LEN);
        }
        else
        {
            MXB_ERROR("Short read when reading the event at %lu in %s. "
                      "Expected %d bytes got %d bytes.",
                      pos,
                      router->binlog_name.c_str(),
                      hdr->event_size - BINLOG_EVENT_HDR_LEN,
                      n);
        }

        result.clear();
    }

    return result;
}

void do_checkpoint(Avro* router)
{
    router->handler->flush();
    avro_save_conversion_state(router);
    AvroSession::notify_all_clients(router->service);
    router->row_count = router->trx_count = 0;
}

static inline REP_HEADER construct_header(uint8_t* ptr)
{
    REP_HEADER hdr;

    hdr.timestamp = mariadb::get_byte4(ptr);
    hdr.event_type = ptr[4];
    hdr.serverid = mariadb::get_byte4(&ptr[5]);
    hdr.event_size = mariadb::get_byte4(&ptr[9]);
    hdr.next_pos = mariadb::get_byte4(&ptr[13]);
    hdr.flags = mariadb::get_byte2(&ptr[17]);

    return hdr;
}

bool read_header(Avro* router, unsigned long long pos, REP_HEADER* hdr, avro_binlog_end_t* rc)
{
    uint8_t hdbuf[BINLOG_EVENT_HDR_LEN];
    int n = pread(router->binlog_fd, hdbuf, BINLOG_EVENT_HDR_LEN, pos);

    /* Read the header information from the file */
    if (n != BINLOG_EVENT_HDR_LEN)
    {
        switch (n)
        {
        case 0:
            break;

        case -1:
            MXB_ERROR("Failed to read binlog file %s at position %llu (%s).",
                      router->binlog_name.c_str(),
                      pos,
                      mxb_strerror(errno));
            break;

        default:
            MXB_ERROR("Short read when reading the header. "
                      "Expected 19 bytes but got %d bytes. "
                      "Binlog file is %s, position %llu",
                      n,
                      router->binlog_name.c_str(),
                      pos);
            break;
        }

        router->current_pos = pos;
        *rc = n == 0 ? AVRO_OK : AVRO_BINLOG_ERROR;
        return false;
    }

    bool rval = true;

    *hdr = construct_header(hdbuf);

    if (hdr->event_type > MAX_EVENT_TYPE_MARIADB10)
    {
        MXB_ERROR("Invalid MariaDB 10 event type 0x%x. Binlog file is %s, position %llu",
                  hdr->event_type,
                  router->binlog_name.c_str(),
                  pos);
        router->current_pos = pos;
        *rc = AVRO_BINLOG_ERROR;
        rval = false;
    }
    else if (hdr->event_size <= 0)
    {
        MXB_ERROR("Event size error: size %d at %llu.", hdr->event_size, pos);
        router->current_pos = pos;
        *rc = AVRO_BINLOG_ERROR;
        rval = false;
    }

    return rval;
}

static bool pos_is_ok(Avro* router, const REP_HEADER& hdr, uint64_t pos)
{
    bool rval = false;

    if (hdr.next_pos > 0 && hdr.next_pos < pos)
    {
        MXB_INFO("Binlog %s: next pos %u < pos %lu, truncating to %lu",
                 router->binlog_name.c_str(),
                 hdr.next_pos,
                 pos,
                 pos);
    }
    else if (hdr.next_pos > 0 && hdr.next_pos != (pos + hdr.event_size))
    {
        MXB_INFO("Binlog %s: next pos %u != (pos %lu + event_size %u), truncating to %lu",
                 router->binlog_name.c_str(),
                 hdr.next_pos,
                 pos,
                 hdr.event_size,
                 pos);
    }
    else if (hdr.next_pos > 0)
    {
        rval = true;
    }
    else
    {
        MXB_ERROR("Current event type %d @ %lu has nex pos = %u : exiting",
                  hdr.event_type,
                  pos,
                  hdr.next_pos);
    }

    return rval;
}

bool read_fde(Avro* router)
{
    bool rval = false;
    avro_binlog_end_t rc;
    REP_HEADER hdr;

    if (read_header(router, 4, &hdr, &rc))
    {
        if (GWBUF result = read_event_data(router, &hdr, 4))
        {
            router->handler->handle_event(hdr, result.data());
            rval = true;
        }
    }
    else if (rc == AVRO_OK)
    {
        // Empty file
        rval = true;
    }

    return rval;
}

/**
 * @brief Read all replication events from a binlog file.
 *
 * Routine detects errors and pending transactions
 *
 * @param router        The router instance
 * @param fix           Whether to fix or not errors
 * @param debug         Whether to enable or not the debug for events
 * @return              How the binlog was closed
 * @see enum avro_binlog_end
 */
avro_binlog_end_t avro_read_all_events(Avro* router)
{
    mxb::WatchdogNotifier::Workaround workaround(mxs::RoutingWorker::get_current());
    mxb_assert(router->binlog_fd != -1);

    if (!read_fde(router))
    {
        MXB_ERROR("Failed to read the FDE event from the binary log: %d, %s",
                  errno,
                  mxb_strerror(errno));
        return AVRO_BINLOG_ERROR;
    }

    uint64_t pos = router->current_pos;
    std::string next_binlog;
    bool rotate_seen = false;

    while (!maxscale_is_shutting_down())
    {
        avro_binlog_end_t rc;
        REP_HEADER hdr;

        if (!read_header(router, pos, &hdr, &rc))
        {
            if (rc == AVRO_OK)
            {
                do_checkpoint(router);

                if (rotate_seen)
                {
                    rotate_to_file(router, pos, next_binlog.c_str());
                }
                else
                {
                    rc = rotate_to_next_file_if_exists(router, pos);
                }
            }
            return rc;
        }

        GWBUF result = read_event_data(router, &hdr, pos);

        if (!result)
        {
            router->current_pos = pos;
            return AVRO_BINLOG_ERROR;
        }

        /* get event content */
        uint8_t* ptr = result.data();

        // These events are only related to binary log files
        if (hdr.event_type == ROTATE_EVENT)
        {
            int len = hdr.event_size - BINLOG_EVENT_HDR_LEN - 8 - (router->handler->have_checksums() ? 4 : 0);
            next_binlog.assign((char*)ptr + 8, len);
            rotate_seen = true;
        }
        else if (hdr.event_type == MARIADB_ANNOTATE_ROWS_EVENT)
        {
            // This appears to need special handling
            int annotate_len = hdr.event_size - BINLOG_EVENT_HDR_LEN
                - (router->handler->have_checksums() ? 4 : 0);
            MXB_INFO("Annotate_rows_event: %.*s", annotate_len, ptr);
            pos += hdr.event_size;
            router->current_pos = pos;
            continue;
        }
        else
        {
            if ((hdr.event_type >= WRITE_ROWS_EVENTv0 && hdr.event_type <= DELETE_ROWS_EVENTv1)
                || (hdr.event_type >= WRITE_ROWS_EVENTv2 && hdr.event_type <= DELETE_ROWS_EVENTv2))
            {
                router->row_count++;
            }
            else if (hdr.event_type == XID_EVENT)
            {
                router->trx_count++;
            }

            router->handler->handle_event(hdr, ptr);
        }

        if (router->row_count >= router->config().row_target
            || router->trx_count >= router->config().trx_target)
        {
            do_checkpoint(router);
        }

        if (pos_is_ok(router, hdr, pos))
        {
            pos = hdr.next_pos;
            router->current_pos = pos;
        }
        else
        {
            break;
        }
    }

    return AVRO_BINLOG_ERROR;
}
