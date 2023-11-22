/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxscale/ccdefs.hh>
#include <maxscale/routingworker.hh>
#include <maxbase/collector.hh>
#include <maxsimd/canonical.hh>

struct QueryEvent
{
    std::string            canonical;
    maxsimd::CanonicalArgs canonical_args;
};

/**
 * @brief The LogContext struct - not used yet
 */
struct RecordContext
{
};

using SharedUpdate = maxbase::SharedData<RecordContext, QueryEvent>;

class WcarRecorder : public maxbase::Collector<SharedUpdate, mxb::CollectorMode::UPDATES_ONLY>
                   , private maxscale::RoutingWorker::Data
{
public:
    WcarRecorder();
private:
    void init_for(maxscale::RoutingWorker* pWorker) override final;
    void finish_for(maxscale::RoutingWorker* pWorker) override final;

    void make_updates(RecordContext*,
                      std::vector<typename SharedUpdate::UpdateType>& queue) override;
};
