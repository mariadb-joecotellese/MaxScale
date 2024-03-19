/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "repcsvstorage.hh"
#include "../capconfig.hh"

namespace
{
std::string quote(std::string str)
{
    mxb_assert(str.find_first_of("\"'") == std::string::npos);
    size_t pos = str.find('\n');

    while (pos != std::string::npos)
    {
        str[pos] = ' ';
        pos = str.find('\n', pos + 1);
    }

    return "\"" + str + "\"";
}
}

// static
void RepCsvStorage::dump_canonicals(Canonicals canonicals, std::ostream& out)
{
    out << "canonical,canonical_sql\n";

    for (const auto& [id, can] : canonicals)
    {
        out << id << "," << quote(*can) << "\n";
    }
}

RepCsvStorage::RepCsvStorage(std::filesystem::path path, Canonicals canonicals, RepConfig::CsvType type)
    : m_type(type)
{
    m_file.open(path.replace_extension("csv"));

    if (!m_file)
    {
        MXB_THROW(WcarError, "Could not open file " << path << ": " << mxb_strerror(errno));
    }

    if (m_type == RepConfig::CsvType::FULL)
    {
        // Store the canonicals for later, we need them to convert the numeric IDs to strings
        m_canonicals = std::move(canonicals);
    }
    else
    {
        // Dump the canonicals with the IDs into a separate file. This mapping will be needed to identify
        // the canonicals from their IDs.
        std::ofstream canonical_file(path.replace_extension("canonicals.csv"));

        if (!canonical_file)
        {
            MXB_THROW(WcarError, "Could not open file " << path << ": " << mxb_strerror(errno));
        }

        dump_canonicals(canonicals, canonical_file);
    }

    m_file << "event_id,canonical,duration,start_time,result_rows,rows_read,error\n";
}

void RepCsvStorage::add_rep_event(RepEvent&& ev)
{
    m_file << ev.event_id << ",";

    if (m_type == RepConfig::CsvType::FULL)
    {
        m_file << quote(*m_canonicals[ev.can_id]) << ",";
    }
    else
    {
        m_file << ev.can_id << ",";
    }

    m_file << mxb::to_secs(ev.end_time - ev.start_time) << ","
           << mxb::to_secs(ev.start_time.time_since_epoch()) << ","
           << ev.num_rows << ","
           << ev.rows_read << ","
           << ev.error << "\n";
}

void RepCsvStorage::add_rep_event(std::vector<RepEvent>& events)
{
    for (auto&& ev : events)
    {
        add_rep_event(std::move(ev));
    }
}

RepStorage::Iterator RepCsvStorage::begin()
{
    return RepStorage::Iterator(this, next_rep_event());
}

RepStorage::Iterator RepCsvStorage::end() const
{
    return RepStorage::Iterator(nullptr, RepEvent {});
}

RepEvent RepCsvStorage::next_rep_event()
{
    MXB_THROW(WcarError, "Not implemented");
}
