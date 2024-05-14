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

var maxscale_host = process.env.maxscale_000_network;
var nosql_port;
var nosql_cache_port;

if (!maxscale_host) {
    console.log("The environment variable 'maxscale_000_network' is not set, " +
                "assuming 127.0.0.1.");
    maxscale_host="127.0.0.1"
    nosql_port = 17017;
    nosql_cache_port = 17018;
}
else
{
    // In the system-test environment we use 4006, which is open. The default
    // nosqlprotocol port of 17017 is not.
    nosql_port = 4006;
    nosql_cache_port = 4007;
}

var timeout;

if (maxscale_host == "127.0.0.1") {
    // We are debugging, so let's set the timeout to an hour.
    timeout = 60 * 60 * 1000;
}
else {
    // Otherwise 10 seconds. 2 seconds is the default, which is too little for some
    // of the tests.
    timeout = 10000;
}

var config = {
    host: maxscale_host,
    mariadb_port: 4008,
    nosql_port: nosql_port,
    nosql_cache_port: nosql_cache_port,
    user: 'maxskysql',
    password: 'skysql'
};

var error = {
    BAD_VALUE: 2,
    NO_SUCH_KEY: 4,
    UNAUTHORIZED: 13,
    TYPE_MISMATCH: 14,
    COMMAND_FAILED: 125
};

const fs = require('fs');
const mariadb = require('mariadb');
const mongodb = require('mongodb')
const assert = require('assert');

var MariaDB = {
    createConnection: async function () {
        var conn = await mariadb.createConnection({
            host: config.host,
            port: config.mariadb_port,
            user: config.user,
            password: config.password });

        conn.fetch_grants_for = async function (name) {
            var results = await this.query("SHOW GRANTS FOR '" + name + "'@'%'");

            var grants = {};

            results.forEach(function (result) {
                // There is just one field in result, so this will give the actual GRANT statment.
                var grant = Object.values(result)[0];
                grant = grant.substring(6); // Strip the initial "GRANT "

                var i = grant.search(" ON ");
                var privileges_string = grant.substring(0, i);
                var privileges = privileges_string.split(', ');

                var resource = grant.substring(i + 4);
                i = resource.search(" TO ");
                resource = resource.substring(0, i);

                grants[resource] = privileges.sort();
            });

            return grants;
        };

        return conn;
    }
}

var MxsMongo = {
    createClient: async function (port) {
        if (!port) {
            port = config.nosql_port
        }

        var uri = "mongodb://" + config.host + ":" + port;
        client = new mongodb.MongoClient(uri, { useUnifiedTopology: true });
        await client.connect();
        return client;
    }
};

class NoSQL {
    constructor(client, db) {
        this.client = client;
        this.db = db;
        this.admin = this.client.db('admin');
    }

    static async create(dbname, port) {
        var client = await MxsMongo.createClient(port);

        if (!dbname) {
            dbname = "nosql";
        }

        var db = client.db(dbname);

        return new NoSQL(client, db);
    }

    dbName() {
        return this.db.s.namespace.db;
    }

    async set_db(dbname) {
        this.db = this.client.db(dbname);
    }

    async close() {
        await this.client.close();
        this.client = null;
        this.db = null;
    }

    async reset(name) {
        try {
            await this.db.command({drop: name});
        }
        catch (x)
        {
            if (x.code != 26) // NameSpace not found
            {
                throw x;
            }
        }
    }

    async find(name, options) {
        var command = {
            find: name
        };

        if (options) {
            for (var p in options) {
                command[p] = options[p];
            }
        }

        return await this.runCommand(command);
    };

    async insert_n(name, n, cb) {
        var documents = [];
        for (var i = 0; i < n; ++i) {
            var doc = {};

            if (cb) {
                cb(doc);
            }
            else {
                doc.i = i;
            };

            documents.push(doc);
        }

        var command = {
            insert: name,
            documents: documents
        };

        return await this.runCommand(command);
    }

    async getLastError() {
        var rv;

        try {
            rv = await this.runCommand({"getLastError": 1});
        }
        catch (x)
        {
            rv = x;
        }

        return rv;
    }

    async deleteAll(name) {
        await this.db.command({delete: name, deletes: [{q: {}, limit: 0}]});
    }

    async runCommand(command) {
        return await this.db.command(command);
    }

    async adminCommand(command) {
        return await this.admin.command(command);
    }

    async ntRunCommand(command) {
        var rv;
        try {
            rv = await this.db.command(command);
        }
        catch (x) {
            rv = x;
        }

        return rv;
    }

    async ntAdminCommand(command) {
        var rv;
        try {
            rv = await this.admin.command(command);
        }
        catch (x) {
            rv = x;
        }

        return rv;
    }

    async delete_cars() {
        await this.deleteAll("cars");
    }

    async insert_cars() {
        const cars = this.db.collection("cars");

        var doc = JSON.parse(fs.readFileSync("test/cars.json", "utf8"));
        var docs = doc.cars;

        const options = { ordered: true };

        return await cars.insertMany(docs, options);
    }
};

var privileges_by_role = {
    // Keep the values in alphabetical order.
    "dbAdmin": {
        db:["ALTER", "CREATE", "DROP", "SELECT" ],
        adminDb: [ "SHOW DATABASES" ]
    },
    "dbOwner": {
        db: ["ALTER", "CREATE", "DELETE", "DROP",
             "INDEX", "INSERT", "SELECT", "UPDATE" ],
        adminDb: [ "CREATE USER", "SHOW DATABASES" ]
    },
    "read": {
        db: ["SELECT"]
    },
    "readWrite": {
        db: ["CREATE", "DELETE", "INDEX", "INSERT", "SELECT", "UPDATE" ]
    },
    "userAdmin": {
        db: ["USAGE"],
    },
    "root": {
        db: ["ALTER", "CREATE", "CREATE USER", "DELETE", "DROP", "INDEX",
             "INSERT", "SELECT", "SHOW DATABASES", "UPDATE" ]
    }
}

module.exports = {
    config,
    mariadb,
    mongodb,
    assert,
    MariaDB,
    NoSQL,
    error,
    timeout,
    privileges_by_role
};
