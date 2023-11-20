/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-10-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#pragma once

#include <maxscale/ccdefs.hh>

#include <array>
#include <mutex>
#include <string>

#include <maxscale/router.hh>
#include <maxscale/protocol/mariadb/module_names.hh>

#include <zlib.h>

#include "writer.hh"
#include "config.hh"
#include "parser.hh"

namespace pinloki
{

class PinlokiSession;

class Pinloki : public mxs::Router
              , public mxb::Worker::Callable
{
public:
    Pinloki(const Pinloki&) = delete;
    Pinloki& operator=(const Pinloki&) = delete;

    static const int64_t CAPABILITIES = RCAP_TYPE_STMT_INPUT | RCAP_TYPE_OLD_PROTOCOL;

    ~Pinloki();
    static Pinloki*     create(SERVICE* pService);
    mxs::RouterSession* newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints) override;
    json_t*             diagnostics() const override;
    uint64_t            getCapabilities() const override;

    mxs::config::Configuration& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_MARIADB_PROTOCOL_NAME};
    }

    bool post_configure();

    const Config&    config() const;
    InventoryWriter* inventory();

    std::string   change_master(const parser::ChangeMasterValues& values);
    bool          is_slave_running() const;
    std::string   start_slave();
    void          stop_slave();
    void          reset_slave();
    GWBUF         show_slave_status(bool all) const;
    mxq::GtidList gtid_io_pos() const;
    void          set_gtid_slave_pos(const maxsql::GtidList& gtid);

private:
    Pinloki(SERVICE* pService);

    bool update_details();

    maxsql::Connection::ConnectionDetails generate_details();

    bool        purge_old_binlogs();
    std::string verify_master_settings();

    struct MasterConfig
    {
        bool        slave_running = false;
        std::string host;
        int64_t     port = 3306;
        std::string user;
        std::string password;
        bool        use_gtid = false;

        bool        ssl = false;
        std::string ssl_ca;
        std::string ssl_capath;
        std::string ssl_cert;
        std::string ssl_crl;
        std::string ssl_crlpath;
        std::string ssl_key;
        std::string ssl_cipher;
        bool        ssl_verify_server_cert = false;

        void save(const Config& config) const;
        bool load(const Config& config);
    };

    Config                  m_config;
    SERVICE*                m_service;
    InventoryWriter         m_inventory;
    std::unique_ptr<Writer> m_writer;
    MasterConfig            m_master_config;
    mxb::Worker::DCId       m_dcid {0};     // Delayed call ID for updating the Writer's connection details
    mutable std::mutex      m_lock;
};

std::pair<std::string, std::string> get_file_name_and_size(const std::string& filepath);
}
