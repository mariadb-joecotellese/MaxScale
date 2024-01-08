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
#include <maxscale/paths.hh>

// Exports to a file
class FileExporter : public ComparatorExporter
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
    }

private:
    int m_fd;
};

std::unique_ptr<ComparatorExporter> build_exporter(const ComparatorConfig& config)
{
    std::unique_ptr<ComparatorExporter> sExporter;

    // TODO: Make this dependent on service being tested.
    std::string file = mxs::datadir();
    file += "/comparator.txt";

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

    return sExporter;
}
