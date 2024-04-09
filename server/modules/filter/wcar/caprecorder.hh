/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capstorage.hh"
#include <maxscale/ccdefs.hh>
#include <maxscale/routingworker.hh>
#include "wcar_collector.hh"
#include <maxsimd/canonical.hh>


class CapRecorder;

/**
 * @brief RecordContext is the data stored in Collector, so will be
 *        garbage collected when Collector is destroyed. It is not
 *        changed or copied in updates-only mode.
 */
class RecorderContext
{
public:
    RecorderContext(Storage* pStorage)
        : pStorage(pStorage)
    {
    }
    Storage* pStorage;

    int64_t bytes_processed() const
    {
        return m_bytes_processed.load(std::memory_order_relaxed);
    }

private:
    friend CapRecorder;
    void update_bytes_processed()
    {
        m_bytes_processed.store(pStorage->size(), std::memory_order_relaxed);
    }
    std::atomic<int64_t> m_bytes_processed = 0;
};

using SharedUpdate = maxbase::SharedData<RecorderContext, QueryEvent>;


class CapRecorder final : public mxb::Collector<SharedUpdate, mxb::CollectorMode::UPDATES_ONLY>
                        , private maxscale::RoutingWorker::Data
{
public:
    CapRecorder(std::unique_ptr<RecorderContext>&& context);

    const RecorderContext& context();

private:
    void init_for(maxscale::RoutingWorker* pWorker) override;
    void finish_for(maxscale::RoutingWorker* pWorker) override;

    void make_updates(RecorderContext* pContext,
                      std::vector<typename SharedUpdate::UpdateType>& events) override;
};
