/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "wcarrecorder.hh"

namespace
{
}

WcarRecorder::WcarRecorder()
    : Collector{std::make_unique<RecordContext>(),
                0,      // Support dynamic thread count
                10000,  // Queue length.
                0}      // Cap, not used in updates_only mode
{
    Data::initialize_workers();
}

void WcarRecorder::init_for(maxscale::RoutingWorker* pWorker)
{
    increase_client_count(pWorker->index());
}

void WcarRecorder::finish_for(maxscale::RoutingWorker* pWorker)
{
    decrease_client_count(pWorker->index());
}

void WcarRecorder::make_updates(RecordContext*, std::vector<SharedUpdate::UpdateType>& queue)
{
}
