/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxbase/ccdefs.hh>

#include "repstorage.hh"
#include <fstream>
#include <filesystem>
#include <map>
#include "repconfig.hh"

class RepCsvStorage final : public RepStorage
{
public:
    using Canonicals = std::map<int64_t, std::shared_ptr<std::string>>;

    /**
     * Construct a RepCsvStorage
     *
     * @param path Path to the input/output file
     * @param type CSV type, must be either FULL or MINIMAL
     */
    RepCsvStorage(std::filesystem::path path, Canonicals canonicals, RepConfig::CsvType type);

    void add_rep_event(RepEvent&& qevent) override;
    void add_rep_event(std::vector<RepEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;

protected:
    RepEvent next_rep_event() override;

private:
    std::ofstream      m_file;
    Canonicals         m_canonicals;
    RepConfig::CsvType m_type;
};
