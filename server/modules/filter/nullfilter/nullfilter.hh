/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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
#pragma once

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>
#include <maxscale/filter.hh>
#include "nullfiltersession.hh"

class NullFilter : public mxs::Filter
{
public:
    NullFilter(const NullFilter&) = delete;
    NullFilter& operator=(const NullFilter&) = delete;

    class Config : public mxs::config::Configuration
    {
    public:
        Config(const std::string& name);

        uint32_t capabilities;
    };

    static NullFilter* create(const char* zName);

    std::shared_ptr<mxs::FilterSession> newSession(MXS_SESSION* pSession, SERVICE* pService) override;

    json_t* diagnostics() const override;

    uint64_t getCapabilities() const override;

    mxs::config::Configuration& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_ANY_PROTOCOL};
    }

private:
    NullFilter(const std::string& name);

private:
    Config m_config;
};
