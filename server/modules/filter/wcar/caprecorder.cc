/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "caprecorder.hh"

namespace
{
}

CapRecorder::CapRecorder(std::unique_ptr<RecorderContext>&& context)
    : Collector{std::move(context),
                0,      // Support dynamic thread count
                10000,  // Queue length.
                0}      // Cap, not used in updates_only mode
{
    Data::initialize_workers();
}

void CapRecorder::init_for(maxscale::RoutingWorker* pWorker)
{
    increase_client_count(pWorker->index());
}

void CapRecorder::finish_for(maxscale::RoutingWorker* pWorker)
{
    decrease_client_count(pWorker->index());
}

void CapRecorder::make_updates(RecorderContext* pContext,
                               std::vector<typename SharedUpdate::UpdateType>& queue)
{
    try
    {
        pContext->pStorage->add_query_event(queue);
    }
    catch (std::exception& ex)
    {
        MXB_SERROR("TODO: unhandled exception " << ex.what());
        throw;
    }
}
