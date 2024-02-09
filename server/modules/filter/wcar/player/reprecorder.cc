/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "reprecorder.hh"

RepRecorder::RepRecorder(std::unique_ptr<RecorderContext>&& context)
    : Collector{std::move(context),
                0,      // Support dynamic thread count
                10000,  // Queue length.
                0}      // Rep, not used in updates_only mode
{
}

void RepRecorder::make_updates(RecorderContext* pContext,
                               std::vector<typename SharedUpdate::UpdateType>& queue)
{
    try
    {
        // TODO add to rep storage
    }
    catch (std::exception& ex)
    {
        MXB_SERROR("TODO: unhandled exception " << ex.what());
        throw;
    }
}
