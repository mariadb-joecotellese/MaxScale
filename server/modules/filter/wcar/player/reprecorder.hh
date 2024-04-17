/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include "repstorage.hh"
#include <maxscale/ccdefs.hh>
#include "../wcar_collector.hh"

struct RecorderContext
{
    RepStorage* pStorage;
    RecorderContext(RepStorage* pStorage)
        : pStorage(pStorage)
    {
    }
};

using SharedUpdate = maxbase::SharedData<RecorderContext, RepEvent>;

class RepRecorder final : public mxb::Collector<SharedUpdate, mxb::CollectorMode::UPDATES_ONLY>
{
public:
    RepRecorder(std::unique_ptr<RecorderContext>&& context, int num_threads);
private:

    void make_updates(RecorderContext* pContext,
                      std::vector<typename SharedUpdate::UpdateType>& events) override;
};
