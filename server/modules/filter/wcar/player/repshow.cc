/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "repshow.hh"
#include "../capbooststorage.hh"

// Base class for anything that filters by a number. Currently only for event IDs but can be extended
// to cover sessions.
class NumberFilter : public ShowFilter
{
public:
    NumberFilter(const RepConfig& config)
    {
        for (const std::string& arg : config.extra_args)
        {
            m_ids.insert(std::stoi(arg));
        }
    }

    bool done() const override
    {
        return m_ids.empty();
    }

protected:
    std::set<uint64_t> m_ids;
};

struct EventShowFilter : public NumberFilter
{
    using NumberFilter::NumberFilter;

    bool operator()(const QueryEvent& ev) override
    {
        if (auto it = m_ids.find(ev.event_id); it != m_ids.end())
        {
            m_ids.erase(it);
            return true;
        }

        return false;
    }
};

class GtidShowFilter : public ShowFilter
{
public:

    GtidShowFilter(const RepConfig& config)
    {
        fs::path trx_path = config.file_name;
        BoostIFile trx_in{trx_path.replace_extension("gx").string()};
        std::set<std::string> gtids(config.extra_args.begin(), config.extra_args.end());

        for (auto&& tevent : CapBoostStorage::load_trx_events(trx_in))
        {
            if (auto it = gtids.find(MAKE_STR(tevent.gtid)); it != gtids.end())
            {
                m_trx.emplace(tevent.session_id, tevent);
                gtids.erase(it);

                if (gtids.empty())
                {
                    break;
                }
            }
        }
    }

    bool operator()(const QueryEvent& ev) override
    {
        if (auto it = m_trx.find(ev.session_id); it != m_trx.end())
        {
            if (ev.event_id >= it->second.start_event_id)
            {
                if (ev.event_id == it->second.end_event_id)
                {
                    m_trx.erase(it);
                }

                return true;
            }
        }

        return false;
    }

    bool done() const override
    {
        return m_trx.empty();
    }

private:
    std::map<uint64_t, TrxEvent> m_trx;
};

namespace
{
bool is_gtid(const std::string& str)
{
    const char* it = str.c_str();
    int dashes = 0;

    // This is not a validation of the GTID, simply a check whether it looks like one.
    while (std::isdigit(*it) || *it == '-')
    {
        dashes += *it == '-';
        ++it;
    }

    return *it == '\0' && dashes == 2;
}

std::unique_ptr<ShowFilter> build_show_filter(const RepConfig& config)
{
    mxb_assert(!config.extra_args.empty());
    size_t num_gtid = std::count_if(config.extra_args.begin(), config.extra_args.end(), is_gtid);

    if (num_gtid > 0 && num_gtid < config.extra_args.size())
    {
        MXB_THROW(WcarError, "a mix of GTIDs and event IDs is not allowed in 'show'.");
    }

    if (num_gtid == config.extra_args.size())
    {
        return std::make_unique<GtidShowFilter>(config);
    }

    return std::make_unique<EventShowFilter>(config);
}
}

RepShow::RepShow(const RepConfig& config)
    : m_config(config)
    , m_filter(build_show_filter(config))
{
}

void RepShow::show(std::ostream& out)
{
    auto& fn = *m_filter;

    for (auto&& qevent : CapBoostStorage(m_config.file_name, ReadWrite::READ_ONLY))
    {
        if (fn(qevent))
        {
            out << qevent << "\n";
        }

        if (m_filter->done())
        {
            break;
        }
    }
}
