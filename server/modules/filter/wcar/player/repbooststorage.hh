/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include "repstorage.hh"
#include "../capbooststorage.hh"

class RepBoostStorage final : public RepStorage
{
public:
    enum Access
    {
        READ_ONLY,
        WRITE_ONLY
    };

    /**
     * Construct a RepBoostStorage
     *
     * @param path   Path to the input/output file
     * @param access Access mode
     */
    RepBoostStorage(const fs::path& base_path, Access access);

    void add_rep_event(RepEvent&& qevent) override;
    void add_rep_event(std::vector<RepEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;

protected:
    RepEvent next_rep_event() override;

private:
    fs::path m_path;

    std::unique_ptr<BoostOFile> m_sRep_event_out;
    std::unique_ptr<BoostIFile> m_sRep_event_in;
};
