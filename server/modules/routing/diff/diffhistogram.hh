/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <set>
#include <vector>

class DiffHistogram
{
public:
    class Specification
    {
    public:
        class Registry
        {
        public:
            Registry() = default;
            Registry(const Registry& other) = default;

            using SpecificationsByCanonical = std::map<std::string, Specification, std::less<>>;

            using iterator = SpecificationsByCanonical::const_iterator;

            void add(std::string_view canonical, const Specification& specification)
            {
                m_specifications_by_canonical.emplace(canonical, specification);
            }

            iterator begin() const
            {
                return m_specifications_by_canonical.begin();
            }

            iterator end() const
            {
                return m_specifications_by_canonical.end();
            }

            iterator find(std::string_view canonical) const
            {
                return m_specifications_by_canonical.find(canonical);
            }

        private:
            SpecificationsByCanonical m_specifications_by_canonical;
        };

        Specification() = default;
        Specification(mxb::Duration min,
                      mxb::Duration delta,
                      int           bins)
            : m_min(min)
            , m_delta(delta)
            , m_bins(bins)
        {
        }

        Specification(const Specification& other) = default;

        bool empty() const
        {
            return m_bins == 0;
        }

        mxb::Duration min() const
        {
            return m_min;
        }

        mxb::Duration delta() const
        {
            return m_delta;
        }

        int bins() const
        {
            return m_bins;
        }

    private:
        mxb::Duration m_min { 0 };
        mxb::Duration m_delta { 0 };
        int           m_bins { 0 };
    };

    struct Bin
    {
        Bin()
        {
        }

        Bin(mxb::Duration l)
            : limit(l)
        {
        }

        mxb::Duration limit {0};
        int64_t       count {0};
        mxb::Duration total {0};
    };

    DiffHistogram(const Specification& specification)
    {
        mxb_assert(specification.bins() >= 2);

        m_bins.reserve(specification.bins());

        for (int i = 0; i <= specification.bins(); ++i)
        {
            m_bins.emplace_back(specification.min() + i * specification.delta());
        }
    }

    DiffHistogram(const DiffHistogram& other) = default;

    int64_t smaller_outliers() const
    {
        return m_nSmaller_outliers;
    }

    int64_t larger_outliers() const
    {
        return m_nLarger_outliers;
    }

    void add(mxb::Duration dur);

    const std::vector<Bin>& bins() const
    {
        return m_bins;
    }

    DiffHistogram& operator += (const DiffHistogram& rhs);

private:
    std::vector<Bin> m_bins;
    int64_t          m_nSmaller_outliers { 0 };
    int64_t          m_nLarger_outliers { 0 };
};
