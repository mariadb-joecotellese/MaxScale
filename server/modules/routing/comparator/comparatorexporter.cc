/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorexporter.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <maxscale/paths.hh>
#include <maxscale/service.hh>
#include <maxscale/utils.hh>

// Exports to a file
class FileExporter final : public CExporter
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

std::unique_ptr<CExporter> build_exporter(const CConfig& config, const mxs::Target& target)
{
    std::unique_ptr<CExporter> sExporter;

    std::string dir = mxs::datadir();
    dir += "/";
    dir += MXB_MODULE_NAME;
    dir += "/";
    dir += config.pService->name();

    if (mxs_mkdir_all(dir.c_str(), 0777))
    {
        time_t now = time(nullptr);
        std::stringstream time;
        time << std::put_time(std::localtime(&now),"%Y-%m-%dT%H-%M-%S");

        std::string file = dir + "/";
        file += config.pMain->name();
        file += "_";
        file += target.name();
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
