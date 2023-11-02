/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratexporter.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <librdkafka/rdkafkacpp.h>

// Exports to maxscale.log on info level
class LogExporter : public UratExporter
{
public:
    void ship(json_t* obj) override final
    {
        MXB_INFO("%s", mxb::json_dump(obj, JSON_COMPACT).c_str());
    }
};

// Exports to a file
class FileExporter : public UratExporter
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

    void ship(json_t* obj) override final
    {
        auto str = mxb::json_dump(obj, JSON_COMPACT) + '\n';
        write(m_fd, str.c_str(), str.length());
    }

private:
    int m_fd;
};

// Exports to a Kafka topic
class KafkaExporter : public UratExporter
{
public:
    KafkaExporter(RdKafka::Producer* producer, const std::string& topic)
        : m_producer(producer)
        , m_topic(topic)
    {
    }

    ~KafkaExporter()
    {
        m_producer->flush(10000);
    }

    void ship(json_t* obj) override final
    {
        char* json = json_dumps(obj, JSON_COMPACT);

        while (m_producer->produce(
                   m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_FREE,
                   json, strlen(json), nullptr, 0, 0, nullptr) == RdKafka::ERR__QUEUE_FULL)
        {
            m_producer->poll(1000);
        }
    }

private:
    std::unique_ptr<RdKafka::Producer> m_producer;
    std::string                        m_topic;
};

std::unique_ptr<UratExporter> build_exporter(const UratConfig& config)
{
    std::unique_ptr<UratExporter> rval;

    switch (config.exporter)
    {
    case ExporterType::EXPORT_LOG:
        rval.reset(new LogExporter);
        break;

    case ExporterType::EXPORT_FILE:
        {
            int fd = open(config.file.c_str(), O_APPEND | O_WRONLY | O_CREAT | O_CLOEXEC,
                          S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

            if (fd != -1)
            {
                rval.reset(new FileExporter(fd));
            }
            else
            {
                MXB_ERROR("Failed to open file '%s', %d, %s", config.file.c_str(), errno,
                          mxb_strerror(errno));
            }
        }
        break;

    case ExporterType::EXPORT_KAFKA:
        {
            std::string err;
            auto cnf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

            if (cnf->set("bootstrap.servers", config.kafka_broker, err) == RdKafka::Conf::ConfResult::CONF_OK)
            {
                if (auto producer = RdKafka::Producer::create(cnf, err))
                {
                    rval.reset(new KafkaExporter(producer, config.kafka_topic));
                }
                else
                {
                    MXB_ERROR("Failed to create Kafka producer: %s", err.c_str());
                }
            }
            else
            {
                MXB_ERROR("Failed to set Kafka parameter `bootstrap.servers`: %s", err.c_str());
            }

            delete cnf;
        }
        break;
    }

    return rval;
}
