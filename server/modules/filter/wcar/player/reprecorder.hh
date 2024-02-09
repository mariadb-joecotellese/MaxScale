/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capstorage.hh"
#include <maxscale/ccdefs.hh>
#include <maxscale/routingworker.hh>
#include <maxbase/collector.hh>

struct RecorderContext
{
    // TODO make class for storing RepEvents to ?
};

using SharedUpdate = maxbase::SharedData<RecorderContext, RepEvent>;


class RepRecorder final : public mxb::Collector<SharedUpdate, mxb::CollectorMode::UPDATES_ONLY>
{
public:
    RepRecorder(std::unique_ptr<RecorderContext>&& context);
private:

    void make_updates(RecorderContext* pContext,
                      std::vector<typename SharedUpdate::UpdateType>& events) override;
};
