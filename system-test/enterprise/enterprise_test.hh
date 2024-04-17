/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxtest/testconnections.hh>

/**
 * Helper macro for declaring a test main function
 */
#define ENTERPRISE_TEST_MAIN(func) \
        int main(int argc, char** argv) { \
            return TestConnections().run_test(argc, argv, [](auto& test) { \
            func(test); \
        }); \
        }
