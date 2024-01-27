/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarplayerconfig.hh"
#include <vector>
#include <unordered_map>

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

/**
 * @brief The Transform class massages the captured data to a suitable form
 *        for the Player to efficiently replay/simulate the workload.
 */
class Transform
{
public:
    Transform(const PlayerConfig* pConfig);
    /**
     * @brief player_storage
     * @return Storage where the events are sorted by start_time
     */
    Storage& player_storage();

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
private:
    void transform_events(Storage& from, Storage& to);

    const PlayerConfig&      m_config;
    std::unique_ptr<Storage> m_player_storage;
    Transactions             m_trxs;
    TrxnMapping              m_trx_start_mapping;
    TrxnMapping              m_trx_end_mapping;
};

inline Transactions& Transform::transactions()
{
    return m_trxs;
}

inline Transactions::iterator Transform::trx_start_mapping(int64_t start_event_id)
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

inline Transactions::iterator Transform::trx_end_mapping(int64_t start_event_id)
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
