/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <maxtest/testconnections.hh>
#include <maxtest/kafka.hh>

#include <maxbase/jansson.hh>

#include <fstream>

enum ChecksumResult
{
    MATCH,
    MISMATCH,
};

enum class Mode
{
    ALL,
    MISMATCHES,
};

struct TestCase
{
    TestCase(const char* q, ChecksumResult r, const char* t)
        : query(q)
        , result(r)
        , type(t)
    {
    }

    std::string    query;
    ChecksumResult result;
    std::string    type;
    uint32_t       id = 0;
};

std::vector<TestCase> test_cases =
{
    {"SELECT 1",                          MATCH,    "resultset"},
    {"SELECT @@hostname",                 MISMATCH, "resultset"},
    {"DO 1",                              MATCH,    "ok"       },
    {"SELECT something that's not valid", MATCH,    "error"    },
};

void check_results(TestConnections& test, json_t* arr, const TestCase& t)
{
    ChecksumResult result = MATCH;
    std::string checksum_value;
    size_t i;
    json_t* value;

    json_array_foreach(arr, i, value)
    {
        json_t* target = json_object_get(value, "target");
        json_t* type = json_object_get(value, "type");
        json_t* rows = json_object_get(value, "rows");
        json_t* warnings = json_object_get(value, "warnings");
        json_t* dur = json_object_get(value, "duration");
        json_t* checksum = json_object_get(value, "checksum");

        if (test.expect(type && json_is_string(type), "Result should contain type string")
            && test.expect(target && json_is_string(target), "Result should contain the target")
            && test.expect(rows && json_is_integer(rows), "Result should contain the row count")
            && test.expect(warnings && json_is_integer(warnings), "Result should contain the warning count")
            && test.expect(dur && json_is_integer(dur), "Result should contain query duration")
            && test.expect(checksum && json_is_string(checksum), "Result should contain the checksum"))
        {
            if (checksum_value.empty())
            {
                checksum_value = json_string_value(checksum);
            }
            else if (checksum_value != json_string_value(checksum))
            {
                result = MISMATCH;
            }

            auto type_str = json_string_value(type);
            test.expect(t.type == type_str, "Expected '%s', got '%s'", t.type.c_str(), type_str);
        }
    }

    test.expect(result == t.result, "Unexpected checksum %s for: %s",
                t.result == MATCH ? "mismatch" : "match", t.query.c_str());
}

void check_json(TestConnections& test, json_t* js, const TestCase& t)
{
    json_t* arr = json_object_get(js, "results");
    json_t* sql = json_object_get(js, "query");
    json_t* cmd = json_object_get(js, "command");
    json_t* ses = json_object_get(js, "session");
    json_t* query_id = json_object_get(js, "query_id");

    if (test.expect(arr && json_is_array(arr), "JSON should contain `results` array")
        && test.expect(sql && json_is_string(sql), "JSON should contain the SQL itself")
        && test.expect(cmd && json_is_string(cmd), "JSON should contain the SQL command")
        && test.expect(ses && json_is_integer(ses), "JSON should contain session ID")
        && test.expect(query_id && json_is_integer(query_id), "JSON should contain query ID"))
    {
        test.expect(json_string_value(sql) == t.query,
                    "SQL mismatch - original: %s result: %s",
                    t.query.c_str(), json_string_value(sql));
        test.expect(json_string_value(cmd) == std::string("COM_QUERY"), "Command mismatch");
        test.expect(json_integer_value(ses) == t.id, "Session ID mismatch");
        test.expect(json_integer_value(query_id) == 1, "Query ID mismatch");
        check_results(test, arr, t);
    }
}

void check_json(TestConnections& test, std::string line, const TestCase& t)
{
    json_error_t err;
    json_t* js = json_loads(line.c_str(), JSON_ALLOW_NUL, &err);

    if (test.expect(js, "JSON should be valid (%s): `%s`", err.text, line.c_str()))
    {
        check_json(test, js, t);

        if (!test.ok())
        {
            char* str = json_dumps(js, JSON_INDENT(2));
            printf("%s\n", str);
            free(str);
        }

        json_decref(js);
    }
}

void run_sql(TestConnections& test)
{
    for (auto& t : test_cases)
    {
        auto conn = test.maxscale->rwsplit();
        test.expect(conn.connect(), "Connection should work: %s", conn.error());
        t.id = conn.thread_id();
        conn.query(t.query);
        conn.disconnect();
    }
}

void test_file(TestConnections& test, Mode mode)
{
    test.check_maxctrl("alter service Mirror-Router"
                       " exporter file"
                       " file /tmp/mirror.txt");

    run_sql(test);

    test.maxscale->stop();
    test.maxscale->copy_from_node("/tmp/mirror.txt", "./mirror.txt");
    test.maxscale->ssh_node_f(true, "rm /tmp/mirror.txt");
    test.maxscale->start();

    std::ifstream infile("mirror.txt");
    std::string line;

    for (const auto& t : test_cases)
    {
        if (mode == Mode::ALL || t.result == MISMATCH)
        {
            if (std::getline(infile, line))
            {
                check_json(test, line, t);
            }
            else
            {
                test.expect(false, "File should not be empty");
            }
        }
    }

    unlink("mirror.txt");
}

void test_kafka(TestConnections& test, Mode mode)
{
    test.check_maxctrl("alter service Mirror-Router"
                       " exporter kafka"
                       " kafka_broker 127.0.0.1:4008"
                       " kafka_topic mirror-topic");

    run_sql(test);
    Consumer consumer(test, "mirror-topic");

    for (const auto& t : test_cases)
    {
        if (mode == Mode::ALL || t.result == MISMATCH)
        {
            auto msg = consumer.consume_one_message();

            if (msg->err() == RdKafka::ERR_NO_ERROR)
            {
                std::string data((const char*)msg->payload(), msg->len());
                check_json(test, data, t);
            }
            else if (msg->err() != RdKafka::ERR__TIMED_OUT
                     && msg->err() != RdKafka::ERR_REQUEST_TIMED_OUT)
            {
                test.add_failure("Failed to consume message: %s", RdKafka::err2str(msg->err()).c_str());
            }
        }
    }

    consumer.commit();
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    Kafka kafka(test);
    kafka.create_topic("mirror-topic");

    test.tprintf("Testing exporter=file, report=always");
    test.check_maxctrl("alter service Mirror-Router report=always");
    test_file(test, Mode::ALL);

    test.tprintf("Testing exporter=file, report=on_conflict");
    test.check_maxctrl("alter service Mirror-Router report=on_conflict");
    test_file(test, Mode::MISMATCHES);

    test.tprintf("Testing exporter=kafka, report=always");
    test.check_maxctrl("alter service Mirror-Router report=always");
    test_kafka(test, Mode::ALL);

    test.tprintf("Testing exporter=kafka, report=on_conflict");
    test.check_maxctrl("alter service Mirror-Router report=on_conflict");
    test_kafka(test, Mode::MISMATCHES);

    return test.global_result;
}
