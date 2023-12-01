/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "wcarrecorder.hh"

namespace
{
}

WcarRecorder::WcarRecorder(std::unique_ptr<RecorderContext>&& context)
    : Collector{std::move(context),
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

void WcarRecorder::make_updates(RecorderContext* pContext, std::vector<SharedUpdate::UpdateType>& events)
{
    try
    {
        pContext->pStorage->add_query_event(events);
    }
    catch (std::exception& ex)
    {
        MXB_SERROR("TODO: unhandled exception " << ex.what());
        throw;
    }
}
