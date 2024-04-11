/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "diffdata.hh"

DiffData::DiffData(const DiffHistogram::Specification& specification)
    : m_histogram(specification)
{
}

void DiffData::add(const mxb::Duration& duration, const mxs::Reply& reply)
{
    m_histogram.add(duration);
}

DiffData& DiffData::operator += (const DiffData& rhs)
{
    m_histogram += rhs.m_histogram;

    return *this;
}
