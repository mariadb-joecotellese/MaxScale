/*
 * Copyright (c) 2021 MariaDB Corporation Ab
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

const assert = require('assert');
const test = require('./nosqltest')
const error = test.error;

const name = "mxsSetConfig";

describe(name, function () {
    this.timeout(test.timeout);

    let nosql;

    /*
     * MOCHA
     */
    before(async function () {
        nosql = await test.NoSQL.create();
    });

    it('Cannot use with non-admin database.', async function () {
        var rv = await nosql.ntRunCommand({mxsSetConfig: {}});

        assert.equal(rv.code, error.UNAUTHORIZED);
    });

    it('Can use with admin database.', async function () {
        var c = {};
        await nosql.adminCommand({mxsSetConfig: c});

        // Valid values
        c.auto_create_databases = true;
        await nosql.adminCommand({mxsSetConfig: c});

        c.auto_create_tables = true;
        await nosql.adminCommand({mxsSetConfig: c});

        c.cursor_timeout = "60s";
        await nosql.adminCommand({mxsSetConfig: c});

        c.debug = "in,out,back";
        await nosql.adminCommand({mxsSetConfig: c});

        c.log_unknown_command = 'true';
        await nosql.adminCommand({mxsSetConfig: c});

        c.on_unknown_command = 'return_empty';
        await nosql.adminCommand({mxsSetConfig: c});
        c.on_unknown_command = 'return_error';
        await nosql.adminCommand({mxsSetConfig: c});

        c.ordered_insert_behavior = "atomic";
        await nosql.adminCommand({mxsSetConfig: c});
        c.ordered_insert_behavior = "default";
        await nosql.adminCommand({mxsSetConfig: c});

        var rv;
        // Invalid values
        c = { auto_create_databases: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.TYPE_MISMATCH);

        c = { auto_create_tables: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.TYPE_MISMATCH);

        c = { cursor_timeout: '10xyzh' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.BAD_VALUE);

        c = { debug: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.BAD_VALUE);

        c = { log_unknown_command: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.BAD_VALUE);

        c = { on_unknown_command: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.BAD_VALUE);

        c = { ordered_insert_behavior: 'blah' };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.BAD_VALUE);

        // Invalid key
        c = { no_such_key: 1 };
        rv = await nosql.ntAdminCommand({mxsSetConfig: c});
        assert.equal(rv.code, error.NO_SUCH_KEY);
    });

    after(async function () {
        if (nosql) {
            await nosql.close();
        }
    });
});
