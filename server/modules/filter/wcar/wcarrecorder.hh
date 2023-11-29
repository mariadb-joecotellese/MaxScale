/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarstorage.hh"
#include <maxscale/ccdefs.hh>
#include <maxscale/routingworker.hh>
#include <maxbase/collector.hh>
#include <maxsimd/canonical.hh>

/**
 * @brief RecordContext is the data stored in GCUpdater, so will be
 *        garbage collected when GCUpdater is destroyed. It is not
 *        changed or copied in updates-only mode.
 */
struct RecorderContext
{
    Storage* pStorage;
    RecorderContext(Storage* pStorage)
        : pStorage(pStorage)
    {
    }
};

using SharedUpdate = maxbase::SharedData<RecorderContext, QueryEvent>;

class WcarRecorder final : public maxbase::Collector<SharedUpdate, mxb::CollectorMode::UPDATES_ONLY>
                         , private maxscale::RoutingWorker::Data
{
public:
    WcarRecorder(std::unique_ptr<RecorderContext>&& context);
private:
    void init_for(maxscale::RoutingWorker* pWorker) override;
    void finish_for(maxscale::RoutingWorker* pWorker) override;

    void make_updates(RecorderContext* pContext,
                      std::vector<typename SharedUpdate::UpdateType>& queue) override;
};
