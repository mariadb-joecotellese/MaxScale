/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffbackend.hh"
#include <map>
#include <vector>

class DiffBinSpecs
{
public:
    DiffBinSpecs() = default;
    DiffBinSpecs(const DiffBinSpecs& other) = default;

    using Bins = std::vector<mxb::Duration>;
    using BinsByCanonical = std::map<std::string, Bins, std::less<>>;

    using iterator = BinsByCanonical::const_iterator;

    void add(std::string_view canonical, const Bins& bins)
    {
        m_bins_by_canonical.emplace(canonical, bins);
    }

    iterator begin() const
    {
        return m_bins_by_canonical.begin();
    }

    iterator end() const
    {
        return m_bins_by_canonical.end();
    }

    iterator find(std::string_view canonical) const
    {
        return m_bins_by_canonical.find(canonical);
    }

private:
    BinsByCanonical m_bins_by_canonical;
};
