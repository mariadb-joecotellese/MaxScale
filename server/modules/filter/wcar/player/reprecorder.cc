/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "reprecorder.hh"

RepRecorder::RepRecorder(std::unique_ptr<RecorderContext>&& context, int num_threads)
    : Collector{std::move(context),
                num_threads,
                512,    // Queue length.
                0,      // Cap, not used in updates_only mode
                mxb::CollecterStopMethod::QUEUES_EMPTY}
{
}

void RepRecorder::make_updates(RecorderContext* pContext,
                               std::vector<typename SharedUpdate::UpdateType>& queue)
{
    try
    {
        pContext->pStorage->add_rep_event(queue);
    }
    catch (std::exception& ex)
    {
        MXB_SERROR("TODO: unhandled exception " << ex.what());
        throw;
    }
}
