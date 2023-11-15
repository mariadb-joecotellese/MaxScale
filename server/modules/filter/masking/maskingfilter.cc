/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

#define MXB_MODULE_NAME "masking"
#include "maskingfilter.hh"

#include <maxscale/json_api.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/paths.hh>
#include <maxscale/utils.hh>

#include "maskingrules.hh"

using std::shared_ptr;
using std::string;

namespace
{

char VERSION_STRING[] = "V1.0.0";

/**
 * Implement "call command masking reload ..."
 *
 * @param pArgs  The arguments of the command.
 *
 * @return True, if the command was handled.
 */
bool masking_command_reload(const MODULECMD_ARG* pArgs, json_t** output)
{
    mxb_assert(pArgs->argc == 1);
    mxb_assert(MODULECMD_GET_TYPE(&pArgs->argv[0].type) == MODULECMD_ARG_FILTER);

    const MXS_FILTER_DEF* pFilterDef = pArgs->argv[0].value.filter;
    mxb_assert(pFilterDef);
    MaskingFilter* pFilter = reinterpret_cast<MaskingFilter*>(filter_def_get_instance(pFilterDef));

    bool rv = false;
    MXS_EXCEPTION_GUARD(rv = pFilter->reload());

    if (!rv)
    {
        MXB_ERROR("Could not reload the rules.");
    }

    return rv;
}
}

//
// Global symbols of the Module
//

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static modulecmd_arg_type_t reload_argv[] =
    {
        {MODULECMD_ARG_FILTER | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Masking name"}
    };

    modulecmd_register_command(MXB_MODULE_NAME,
                               "reload",
                               MODULECMD_TYPE_ACTIVE,
                               masking_command_reload,
                               MXS_ARRAY_NELEMS(reload_argv),
                               reload_argv,
                               "Reload masking filter rules");

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::IN_DEVELOPMENT,
        MXS_FILTER_VERSION,
        "A masking filter that is capable of masking/obfuscating returned column values.",
        "V1.0.0",
        RCAP_TYPE_STMT_INPUT | RCAP_TYPE_STMT_OUTPUT | RCAP_TYPE_OLD_PROTOCOL,
        &mxs::FilterApi<MaskingFilter>::s_api,
        NULL,   /* Process init. */
        NULL,   /* Process finish. */
        NULL,   /* Thread init. */
        NULL,   /* Thread finish. */
    };

    static bool populated = false;

    if (!populated)
    {
        MaskingFilterConfig::populate(info);
        populated = true;
    }

    return &info;
}

//
// MaskingFilter
//

MaskingFilter::MaskingFilter(const char* zName)
    : m_config(zName, *this)
{
    MXB_NOTICE("Masking filter [%s] created.", m_config.name().c_str());
}

MaskingFilter::~MaskingFilter()
{
}

// static
MaskingFilter* MaskingFilter::create(const char* zName)
{
    return new MaskingFilter(zName);
}

MaskingFilterSession* MaskingFilter::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    return MaskingFilterSession::create(pSession, pService, this);
}

// static
json_t* MaskingFilter::diagnostics() const
{
    return NULL;
}

// static
uint64_t MaskingFilter::getCapabilities() const
{
    return RCAP_TYPE_STMT_INPUT | RCAP_TYPE_STMT_OUTPUT | RCAP_TYPE_OLD_PROTOCOL;
}

bool MaskingFilter::reload()
{
    bool rval = m_config.reload_rules();
    const auto& cnf = config();

    if (rval)
    {
        MXB_NOTICE("Rules for masking filter '%s' were reloaded from '%s'.",
                   m_config.name().c_str(), cnf.rules.c_str());
    }
    else
    {
        MXB_ERROR("Rules for masking filter '%s' could not be reloaded from '%s'.",
                  m_config.name().c_str(), cnf.rules.c_str());
    }

    return rval;
}
