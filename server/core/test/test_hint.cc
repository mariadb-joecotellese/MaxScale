/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <cstdio>
#include <maxscale/hint.hh>

int main(int argc, char** argv)
{
    int result = 0;
    fprintf(stderr, "testhint : Create a parameter hint");
    Hint hint("name", "value");
    if (hint.value != "value")
    {
        fprintf(stderr, "Hint value should be correct\n");
        result++;
    }
    if (hint.type != Hint::Type::PARAMETER)
    {
        fprintf(stderr, "Hint type should be 'parameter'\n");
        result++;
    }
    fprintf(stderr, "\t..done\n");
    return result;
}
