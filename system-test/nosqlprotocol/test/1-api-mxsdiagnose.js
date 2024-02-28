/*
 * Copyright (c) 2021 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

const assert = require('assert');
const test = require('./nosqltest')
const error = test.error;

const name = "mxsDiagnose";

describe(name, function () {
    this.timeout(test.timeout);

    let nosql;

    /*
     * MOCHA
     */
    before(async function () {
        nosql = await test.NoSQL.create();
    });

    it('Works.', async function () {
        // Valid command.
        var rv = await nosql.runCommand({mxsDiagnose: { ping: 1 }});

        assert.equal(rv.ok, 1);
        assert.equal(rv.error, undefined);
        assert.notEqual(rv.kind, undefined);
        assert.notEqual(rv.response, undefined);

        // Invalid command.
        var rv = await nosql.runCommand({mxsDiagnose: { pingX: 1 }});

        assert.equal(rv.ok, 1);
        assert.notEqual(rv.error, undefined);
        assert.equal(rv.kind, undefined);
        assert.equal(rv.response, undefined);
    });

    after(async function () {
        if (nosql) {
            await nosql.close();
        }
    });
});
