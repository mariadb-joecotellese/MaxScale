/*
 * Copyright (c) 2021 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

const assert = require('assert');
const test = require('./nosqltest')
const error = test.error;

const name = "mxsCreateDatabase";

describe(name, function () {
    this.timeout(test.timeout);

    let nosql;
    let conn;

    var random_db = name + Math.random().toString(10).substring(2);

    /*
     * MOCHA
     */
    before(async function () {
        nosql = await test.NoSQL.create();
        conn = await test.MariaDB.createConnection();
    });

    it('Cannot create a database using non-admin database.', async function () {
        var rv = await nosql.ntRunCommand({mxsCreateDatabase: random_db});

        assert.equal(rv.code, error.UNAUTHORIZED);
    });

    it('Can create a database using admin database.', async function () {
        await nosql.close();
        nosql = undefined;

        await conn.query("DROP DATABASE IF EXISTS " + random_db);

        nosql = await test.NoSQL.create("admin");

        var rv = await nosql.runCommand({mxsCreateDatabase: random_db});
        assert.equal(rv.ok, 1);

        await conn.query("USE " + random_db);
        await conn.query("DROP DATABASE " + random_db);
    });

    after(async function () {
        if (nosql) {
            await nosql.close();
        }

        if (conn) {
            await conn.end();
        }
    });
});
