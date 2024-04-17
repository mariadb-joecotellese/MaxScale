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
                512,    // Queue length.
                0,      // Cap, not used in updates_only mode
                mxb::CollecterStopMethod::QUEUES_EMPTY}
{
    Data::initialize_workers();
}

const RecorderContext& CapRecorder::context()
{
    return *get_pLatest();      // TODO add to Collector. Valid for CollectorMode::UPDATES_ONLY
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

        pContext->update_bytes_processed();
    }
    catch (std::exception& ex)
    {
        MXB_SERROR("TODO: unhandled exception " << ex.what());
        throw;
    }
}
