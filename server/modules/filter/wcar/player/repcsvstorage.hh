/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include "repstorage.hh"
#include <fstream>
#include <filesystem>
#include <map>

class RepCsvStorage final : public RepStorage
{
public:
    using Canonicals = std::map<int64_t, std::shared_ptr<std::string>>;

    /**
     * Construct a RepCsvStorage
     *
     * @param path       Path to the input/output file
     * @param canonicals The mapping of canonical IDs to their SQL. If empty, the numeric IDs are used.
     */
    RepCsvStorage(std::filesystem::path path, Canonicals canonicals = {});

    /**
     * Dump the canonicals as CSV
     *
     * @param canonicals The canonicals to dump
     * @param out        The stream where the dump is written
     */
    static void dump_canonicals(Canonicals canonicals, std::ostream& out);

    void add_rep_event(RepEvent&& qevent) override;
    void add_rep_event(std::vector<RepEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;

protected:
    RepEvent next_rep_event() override;

private:
    std::ofstream m_file;
    Canonicals    m_canonicals;
};
