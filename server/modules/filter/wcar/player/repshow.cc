/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "repshow.hh"

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

namespace
{
std::unique_ptr<ShowFilter> build_show_filter(const RepConfig& config)
{
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
            out << maxsimd::canonical_args_to_sql(*qevent.sCanonical, qevent.canonical_args) << ";\n";
        }

        if (m_filter->done())
        {
            break;
        }
    }
}
