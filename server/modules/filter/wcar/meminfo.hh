/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxscale/ccdefs.hh>
#include <fstream>
#include <sstream>

class MemInfo
{
public:
    MemInfo()
    {
        update();
    }

    unsigned long totalram()
    {
        return m_totalram;
    }

    unsigned long memavailable()
    {
        return m_memavailable;
    }

    float free_pct()
    {
        // Could use cheaper sysinfo(), and call update only if needed
        update();
        return 100.0 * m_memavailable / m_totalram;
    }

    void update()
    {
        m_totalram = 0;
        m_memavailable = 0;
        std::ifstream is{"/proc/meminfo"};
        std::string line;
        while (std::getline(is, line))
        {
            std::istringstream oss{line};
            std::string label, value, unit;
            oss >> label;
            oss >> value;
            oss >> unit;
            if (label == "MemTotal:"s)
            {
                m_totalram = convert(value, unit);
            }
            else if (label == "MemAvailable:"s)
            {
                m_memavailable = convert(value, unit);
            }

            if (m_totalram && m_memavailable)
            {
                break;
            }
        }
    }
private:

    int64_t convert(const std::string& value, const std::string& unit)
    {
        int64_t ret = std::stoll(value);
        if (unit == "kB"s)
        {
            ret *= 1024;
        }
        else if (unit == "mB"s)
        {
            ret *= 1024 * 1024;
        }
        else if (unit == "gB"s)
        {
            ret *= 1024 * 1024 * 1024;
        }

        return ret;
    }

    int64_t m_totalram = 0;
    int64_t m_memavailable = 0;
};
