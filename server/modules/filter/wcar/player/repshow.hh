/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include <maxbase/ccdefs.hh>

#include "repconfig.hh"
#include "../capbooststorage.hh"


struct ShowFilter
{
    virtual ~ShowFilter() = default;
    virtual bool operator()(const QueryEvent& ev) = 0;
    virtual bool done() const = 0;
};

class RepShow
{
public:
    RepShow(const RepConfig& config);
    void show(std::ostream& out);

private:
    const RepConfig&            m_config;
    std::unique_ptr<ShowFilter> m_filter;
};
