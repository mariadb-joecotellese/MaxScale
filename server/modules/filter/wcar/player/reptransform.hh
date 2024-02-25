/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "repconfig.hh"
#include "../capstorage.hh"
#include <vector>
#include <unordered_map>
#include <filesystem>

struct Transaction
{
    int64_t        session_id = -1;
    int64_t        start_event_id;
    int64_t        end_event_id;
    mxb::TimePoint end_time;
    bool           completed = false;

    bool is_valid()
    {
        return session_id != -1;
    }
};

using Transactions = std::vector<Transaction>;
// Keyed by event_id
using TrxnMapping = std::unordered_map<int64_t, Transactions::iterator>;

namespace fs = std::filesystem;

/**
 * @brief The RepTransform class massages the captured data to a suitable form
 *        for the Player to efficiently replay/simulate the workload.
 */
class RepTransform
{
public:
    RepTransform(const RepConfig* pConfig);
    /**
     * @brief  player_storage
     * @return Storage where the events are sorted by start_time
     */
    Storage& player_storage();

    /**
     * @brief  rep_event_storage
     * @return Storage to where RepEvents are written
     */
    Storage& rep_event_storage();

    /**
     * @brief finalize to be called after replay has finished (save rep events)
     */
    void finalize();

    /**
     * @brief transactions
     * @return All the transaction sorted by their end_time. This means that the
     *         front transaction is the only one that matters for scheduling
     *         single events: events that start before the end of the trxn can be
     *         scheduled, while events that start after the transaction have to pend.
     *         Events inside a transaction (and a session) do not depend
     *         on other transactions, so they are free to be scheduled.
     *
     *         The access methods are non-const purely for the reason that the player
     *         will set Transaction::complete as it executes events.
     */
    Transactions&          transactions();
    Transactions::iterator trx_start_mapping(int64_t start_event_id);
    Transactions::iterator trx_end_mapping(int64_t start_event_id);

    int max_parallel_sessions() const
    {
        return m_max_parallel_sessions;
    }
private:
    void transform_events(const fs::path& path);

    const RepConfig&         m_config;
    std::unique_ptr<Storage> m_player_storage;
    std::unique_ptr<Storage> m_rep_event_storage;
    Transactions             m_trxs;
    TrxnMapping              m_trx_start_mapping;
    TrxnMapping              m_trx_end_mapping;
    int                      m_max_parallel_sessions;
};

inline Storage& RepTransform::player_storage()
{
    return *m_player_storage;
}

inline Storage& RepTransform::rep_event_storage()
{
    return *m_rep_event_storage;
}

inline Transactions& RepTransform::transactions()
{
    return m_trxs;
}

inline Transactions::iterator RepTransform::trx_start_mapping(int64_t start_event_id)
{
    auto ite = m_trx_start_mapping.find(start_event_id);
    if (ite == end(m_trx_start_mapping))
    {
        return end(m_trxs);
    }
    else
    {
        return ite->second;
    }
}

inline Transactions::iterator RepTransform::trx_end_mapping(int64_t start_event_id)
{
    auto ite = m_trx_end_mapping.find(start_event_id);
    if (ite == end(m_trx_end_mapping))
    {
        return end(m_trxs);
    }
    else
    {
        return ite->second;
    }
}
