/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffexporter.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <maxscale/paths.hh>
#include <maxscale/service.hh>
#include <maxscale/utils.hh>

// Exports to a file
class FileExporter final : public DiffExporter
{
public:
    FileExporter(int fd)
        : m_fd(fd)
    {
    }

    ~FileExporter()
    {
        close(m_fd);
    }

    void ship(json_t* pJson) override final
    {
        auto str = mxb::json_dump(pJson, JSON_COMPACT) + '\n';
        write(m_fd, str.c_str(), str.length());
        json_decref(pJson);
    }

private:
    int m_fd;
};

std::unique_ptr<DiffExporter> build_exporter(const std::string& diff_service_name,
                                             const mxs::Target& main_target,
                                             const mxs::Target& other_target)
{
    std::unique_ptr<DiffExporter> sExporter;

    std::string dir = mxs::datadir();
    dir += "/";
    dir += MXB_MODULE_NAME;
    dir += "/";
    dir += diff_service_name;

    if (mxs_mkdir_all(dir.c_str(), 0777))
    {
        time_t now = time(nullptr);
        std::stringstream time;
        time << std::put_time(std::localtime(&now),"%Y-%m-%d_%H%M%S");

        std::string file = dir + "/";
        file += main_target.name();
        file += "_";
        file += other_target.name();
        file += "_";
        file += time.str();
        file += ".json";

        int fd = open(file.c_str(), O_APPEND | O_WRONLY | O_CREAT | O_CLOEXEC,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

        if (fd != -1)
        {
            sExporter.reset(new FileExporter(fd));
        }
        else
        {
            MXB_ERROR("Failed to open file '%s', %d, %s", file.c_str(), errno,
                      mxb_strerror(errno));
        }
    }

    return sExporter;
}
