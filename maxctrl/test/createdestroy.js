const {
  startMaxScale,
  stopMaxScale,
  doCommand,
  verifyCommand,
  createConnection,
  getConnectionId,
  doQuery,
  closeConnection,
  expect,
} = require("../test_utils.js");

const fs = require("fs");

describe("Create/Destroy Commands", function () {
  // The other test cases modify the runtime configuration. Run the session destroy tests
  // so that we don't restart MaxScale before it.
  it("destroys session", async function () {
    await createConnection();
    var id = getConnectionId();
    await doCommand("destroy session " + id);
    await doQuery("SELECT").should.be.rejected;
    await closeConnection();
  });

  it("destroys session with TTL", async function () {
    await createConnection();
    var id = getConnectionId();
    await doCommand("destroy session " + id + " --ttl 1");
    await doQuery("SELECT 1");
    await doQuery("SELECT SLEEP(3)").should.be.rejected;
    await closeConnection();
  });

  it("does not destroy unknown session", async function () {
    await doCommand("destroy session 123456").should.be.rejected;
  });

  it("create monitor", function () {
    return verifyCommand(
      "create monitor my-monitor mysqlmon user=maxuser password=maxpwd",
      "monitors/my-monitor"
    ).should.be.fulfilled;
  });

  it("destroy monitor", function () {
    return doCommand("destroy monitor my-monitor").should.be.fulfilled.then(() =>
      doCommand("show monitor my-monitor")
    ).should.be.rejected;
  });

  it("monitor without parameters fails due to missing user parameter", function () {
    return doCommand("create monitor my-monitor mysqlmon").should.be.rejected;
  });

  it("destroy monitor created without parameters", function () {
    return doCommand("destroy monitor my-monitor").should.be.fulfilled.then(() =>
      doCommand("show monitor my-monitor")
    ).should.be.rejected;
  });

  it("will not destroy the same monitor again", function () {
    return doCommand("destroy monitor my-monitor").should.be.rejected;
  });

  it("will not destroy nonexistent monitor", function () {
    return doCommand("destroy monitor monitor123").should.be.rejected;
  });

  it("will not create monitor with bad parameters", function () {
    return doCommand("create monitor my-monitor some-module").should.be.rejected;
  });

  it("will not create monitor with bad options", function () {
    return doCommand("create monitor my-monitor mysqlmon --this-is-not-an-option").should.be.rejected;
  });

  it("will not create monitor with malformed parameters", function () {
    return doCommand("create monitor my-monitor mariadbmon not-a-param").should.be.rejected;
  });

  it("will not create monitor with duplicate servers", async function () {
    await doCommand("create server test_server1 127.0.0.1 6789");
    await doCommand(
      "create monitor my-monitor mariadbmon user=maxuser password=maxpwd --servers test_server1 test_server1"
    ).should.be.rejected;
    await doCommand(
      "create monitor my-monitor mariadbmon user=maxuser password=maxpwd --servers test_server1"
    );
    await doCommand("destroy monitor my-monitor --force");
    await doCommand("destroy server test_server1 --force");
  });

  it("create monitor with options", function () {
    return doCommand("unlink monitor MariaDB-Monitor server4")
      .then(() =>
        verifyCommand(
          "create monitor my-monitor mysqlmon user=maxuser password=maxpwd --servers server4",
          "monitors/my-monitor"
        )
      )
      .then(function (res) {
        res.data.relationships.servers.data.length.should.equal(1);
        res.data.relationships.servers.data[0].id.should.equal("server4");
        res.data.attributes.parameters.user.should.equal("maxuser");
        res.data.attributes.parameters.password.should.equal("*****");
      });
  });

  it("will not create already existing monitor", function () {
    return doCommand("create monitor my-monitor mysqlmon").should.be.rejected;
  });

  it("will not create server without parameters", function () {
    return doCommand("create server no-param-server").should.be.rejected;
  });

  it("will not create server without an address", function () {
    return doCommand("create server missing-address-server proxy_protocol=true").should.be.rejected;
  });

  it("create server", function () {
    return verifyCommand("create server server5 127.0.0.1 3004", "servers/server5").should.be.fulfilled;
  });

  it("destroy server", function () {
    return doCommand("destroy server server5").should.be.fulfilled;
  });

  it("create server with custom parameters", async function () {
    var res = await verifyCommand("create server server5 127.0.0.1 3004 extra_port=4004", "servers/server5");
    res.data.attributes.parameters.extra_port.should.equal(4004);
    await doCommand("destroy server server5");
  });

  it("will not create server with bad parameters", function () {
    return doCommand("create server server5 bad parameter").should.be.rejected;
  });

  it("will not create server with bad custom parameters", function () {
    return doCommand("create server server5 127.0.0.1 4567 bad=parameter").should.be.rejected;
  });

  it("will not create server with a bad key-value parameter", function () {
    return doCommand("create server kv-server address=127.0.0.1 port4567 bad=parameter").should.be.rejected;
  });

  it("will not create server with garbage after key-value parameter", function () {
    return doCommand("create server kv-server address=127.0.0.1 3306").should.be.rejected;
  });

  it("will not create server if 'monitorpw' is missing", function () {
    return doCommand("create server kv-server address=127.0.0.1 monitoruser=myuser").should.be.rejected;
  });

  it("will not create server with bad options", function () {
    return doCommand("create server server5 bad parameter --this-is-not-an-option").should.be.rejected;
  });

  it("create server for service and monitor", function () {
    return verifyCommand(
      "create server server6 127.0.0.1 3005 --services RW-Split-Router --monitors MariaDB-Monitor",
      "servers/server6"
    ).then(function (res) {
      res.data.relationships.services.data[0].id.should.equal("RW-Split-Router");
      res.data.relationships.services.data.length.should.equal(1);
      res.data.relationships.monitors.data[0].id.should.equal("MariaDB-Monitor");
      res.data.relationships.monitors.data.length.should.equal(1);
    });
  });

  it("will not create already existing server", function () {
    return doCommand("create server server1 127.0.0.1 3000").should.be.rejected;
  });

  it("will not destroy nonexistent server", function () {
    return doCommand("destroy server server123").should.be.rejected;
  });

  it("create and destroy server with socket", function () {
    return verifyCommand("create server server7 /tmp/server.sock", "servers/server7").then(() =>
      doCommand("destroy server server7")
    ).should.be.fulfilled;
  });

  it("creates a server with address as the only key-value parameter", async function () {
    await verifyCommand("create server kv-server address=127.0.0.1", "servers/kv-server");
    await doCommand("destroy server kv-server");
  });

  it("creates a server with socket as the only key-value parameter", async function () {
    await verifyCommand("create server kv-server socket=/tmp/server.sock", "servers/kv-server");
    await doCommand("destroy server kv-server");
  });

  it("creates a server with address and port as key-value parameters", async function () {
    await verifyCommand("create server kv-server address=127.0.0.1 port=3306", "servers/kv-server");
    await doCommand("destroy server kv-server");
  });

  it("creates a server with key-value parameters in reverse", async function () {
    await verifyCommand("create server kv-server port=3306 address=127.0.0.1", "servers/kv-server");
    await doCommand("destroy server kv-server");
  });

  it("creates a server with address and port as key-value parameters", async function () {
    await verifyCommand("create server kv-server address=127.0.0.1 port=3306", "servers/kv-server");
    await doCommand("destroy server kv-server");
  });

  it("create listener", function () {
    return verifyCommand(
      "create listener RW-Split-Router my-listener 4567",
      "services/RW-Split-Router/listeners/my-listener"
    ).should.be.fulfilled;
  });

  it("will not create already existing listener", function () {
    return doCommand("create listener RW-Split-Router my-listener 7890").should.be.rejected;
  });

  it("will not create listener with already used port", function () {
    return doCommand("create listener RW-Split-Router my-listener2 4567").should.be.rejected;
  });

  it("will not create listener with negative port", function () {
    return doCommand("create listener RW-Split-Router my-listener3 -123").should.be.rejected;
  });

  it("will not create listener with port that is not a number", function () {
    return doCommand("create listener RW-Split-Router my-listener3 any-port-is-ok").should.be.rejected;
  });

  it("destroy listener", function () {
    return doCommand("destroy listener RW-Split-Router my-listener").should.be.fulfilled;
  });

  it("will not destroy static listener", function () {
    return doCommand("destroy listener RW-Split-Router RW-Split-Listener").should.be.fulfilled;
  });

  it("create listener", async function () {
    var res = await verifyCommand(
      "create listener RW-Split-Router my-listener 4567 address=127.0.0.1",
      "services/RW-Split-Router/listeners/my-listener"
    );

    res.data.attributes.parameters.address.should.equal("127.0.0.1");

    await doCommand("destroy listener RW-Split-Router my-listener");
  });

  it("create user", function () {
    return verifyCommand("create user testuser test", "users/inet/testuser");
  });

  it("destroy user", function () {
    return doCommand("destroy user testuser");
  });

  it("create user with numbers as a name", function () {
    return verifyCommand("create user 1234 1234", "users/inet/1234").then(() =>
      doCommand("destroy user 1234")
    );
  });

  it("create admin user", function () {
    return verifyCommand("create user testadmin test --type=admin", "users/inet/testadmin").then((res) => {
      res.data.attributes.account.should.equal("admin");
    });
  });

  it("destroy admin user", function () {
    return doCommand("destroy user testadmin");
  });

  it("create basic user", function () {
    return verifyCommand("create user testbasic test --type=basic", "users/inet/testbasic").then((res) => {
      res.data.attributes.account.should.equal("basic");
    });
  });

  it("destroy basic user", function () {
    return doCommand("destroy user testbasic");
  });

  it("create user with bad type", function () {
    return doCommand("create user testadmin test --type=superuser").should.be.rejected;
  });

  it("create service with bad parameter", function () {
    return doCommand("create service test-service readwritesplit user-not-required").should.be.rejected;
  });

  it("create service", function () {
    return verifyCommand(
      "create service test-service readwritesplit user=maxuser password=maxpwd",
      "services/test-service"
    ).should.be.fulfilled;
  });

  it("destroy service", function () {
    return doCommand("destroy service test-service").should.be.fulfilled;
  });

  it("create service with server relationship", function () {
    return doCommand("create server test-server 127.0.0.1 3306").then(() =>
      verifyCommand(
        "create service test-service readwritesplit user=maxuser password=maxpwd --servers test-server",
        "services/test-service"
      )
    ).should.be.fulfilled;
  });

  it("destroy service with server relationships", function () {
    return doCommand("destroy service test-service")
      .should.be.rejected.then(() => doCommand("unlink service test-service test-server"))
      .then(() => doCommand("destroy service test-service")).should.be.fulfilled;
  });

  it("create service with filter relationship", function () {
    return doCommand("create filter test-filter-1 qlafilter filebase=/tmp/qla")
      .then(() =>
        verifyCommand(
          "create service test-service-2 readwritesplit user=maxuser password=maxpwd --filters test-filter-1",
          "services/test-service-2"
        )
      )
      .then((res) => {
        res.data.relationships.filters.data.length.should.equal(1);
      });
  });

  it("destroy service with filter relationships", function () {
    return doCommand("destroy service test-service-2")
      .should.be.rejected.then(() => doCommand("alter service-filters test-service-2"))
      .then(() => doCommand("destroy service test-service-2")).should.be.fulfilled;
  });

  it("create filter with bad parameters", function () {
    return doCommand("create filter test-filter qlafilter filebase-not-required").should.be.rejected;
  });

  it("create filter", function () {
    return verifyCommand("create filter test-filter qlafilter filebase=/tmp/qla.log", "filters/test-filter")
      .should.be.fulfilled;
  });

  it("destroy filter", function () {
    return doCommand("destroy filter test-filter").should.be.fulfilled;
  });

  it("create filter with no parameters", function () {
    return verifyCommand("create filter test-filter hintfilter", "filters/test-filter").should.be.fulfilled;
  });

  it("destroy filter with no parameters", function () {
    return doCommand("destroy filter test-filter").should.be.fulfilled;
  });

  it("create filter with bad parameters", function () {
    return doCommand("create filter test-filter qlafilter count 10").should.be.rejected;
  });

  it("create filter with equals sign in parameters", function () {
    return verifyCommand(
      "create filter test-filter regexfilter match=/this=is=a=test/ replace=test-passed",
      "filters/test-filter"
    ).then(() => doCommand("destroy filter test-filter")).should.be.fulfilled;
  });

  it("detects filter->service dependency", async function () {
    await doCommand(
      "create service tee-target readconnroute router_options=master user=maxuser password=maxpwd"
    );
    await doCommand("create filter tee-filter tee target=tee-target");
    await doCommand("destroy service tee-target").should.be.rejected;
    await doCommand("destroy filter tee-filter");
    await doCommand("destroy service tee-target");
  });

  it("detects filter->server dependency", async function () {
    await doCommand("create server tee-target 127.0.0.1 3006");
    await doCommand("create filter tee-filter tee target=tee-target");
    await doCommand("destroy server tee-target").should.be.rejected;
    await doCommand("destroy filter tee-filter");
    await doCommand("destroy server tee-target");
  });

  it("detects service->service dependency", async function () {
    await doCommand(
      "create service child-service readconnroute router_options=master user=maxuser password=maxpwd"
    );
    await doCommand(
      "create service parent-service readconnroute router_options=master user=maxuser password=maxpwd"
    );
    await doCommand("link service parent-service child-service");
    await doCommand("destroy service child-service").should.be.rejected;
    await doCommand("unlink service parent-service child-service");
    await doCommand("destroy service parent-service");
    await doCommand("destroy service child-service");
  });

  it("generates a diagnostic report", async function () {
    if (fs.existsSync("report.txt")) {
      fs.rmSync("report.txt");
    }

    await doCommand("create report report.txt");
    const data = fs.readFileSync("report.txt", { encoding: "utf8" });
    const js = JSON.parse(data);
    expect(js.maxscale).to.be.an("object");
  });

  it("writes diagnostic report to stdout", async function () {
    var res = await doCommand("create report");
    expect(res).to.be.a("string");
    var js = JSON.parse(res);
    expect(js).to.be.an("object");
    expect(js.maxscale).to.be.an("object");
  });

  after(async function () {
    await stopMaxScale();
    await startMaxScale();
  });
});
