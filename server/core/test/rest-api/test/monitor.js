require("../utils.js")();

const monitor = {
  data: {
    id: "test-monitor",
    type: "monitors",
    attributes: {
      module: "mysqlmon",
      parameters: {
        user: "maxuser",
        password: "maxpwd",
      },
    },
  },
};

describe("Monitor", function () {

  describe("Actions", function () {
    it("stop monitor", function () {
      return request
        .put(base_url + "/monitors/MariaDB-Monitor/stop")
        .then(function () {
          return request.get(base_url + "/monitors/MariaDB-Monitor");
        })
        .then(function (mon) {
          mon.data.attributes.state.should.be.equal("Stopped");
        });
    });

    it("start monitor", function () {
      return request
        .put(base_url + "/monitors/MariaDB-Monitor/start")
        .then(function () {
          return request.get(base_url + "/monitors/MariaDB-Monitor");
        })
        .then(function (mon) {
          mon.data.attributes.state.should.be.equal("Running");
        });
    });
  });

  describe("Creation", function () {
    it("create new monitor", function () {
      return request.post(base_url + "/monitors/", { json: monitor }).should.be.fulfilled;
    });

    it("request monitor", function () {
      return request.get(base_url + "/monitors/" + monitor.data.id).should.be.fulfilled;
    });

    it("alter monitor", function () {
      var obj = { data: { attributes: { parameters: { monitor_interval: 1000 } } } };
      return request.patch(base_url + "/monitors/" + monitor.data.id, { json: monitor }).should.be.fulfilled;
    });

    it("destroy created monitor", function () {
      return request.delete(base_url + "/monitors/" + monitor.data.id).should.be.fulfilled;
    });
  });

  describe("Relationships", function () {
    it("create new monitor", function () {
      return request.post(base_url + "/monitors/", { json: monitor }).should.be.fulfilled;
    });

    it("remove with malformed relationships", function () {
      var mon = { data: { relationships: { servers: null } } };
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor", { json: mon })
        .should.be.rejected.then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.should.have.keys("servers");
        });
    });

    it("missing relationships are not removed", function () {
      var mon = { data: { relationships: {} } };
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor", { json: mon })
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.should.have.keys("servers");
        });
    });

    it("remove relationships from old monitor", function () {
      var mon = { data: { relationships: { servers: { data: null } } } };
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor", { json: mon })
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.should.not.have.keys("servers");
        });
    });

    it("add relationships to new monitor", function () {
      var mon = {
        data: {
          relationships: {
            servers: {
              data: [
                { id: "server1", type: "servers" },
                { id: "server2", type: "servers" },
                { id: "server3", type: "servers" },
                { id: "server4", type: "servers" },
              ],
            },
          },
        },
      };
      return request
        .patch(base_url + "/monitors/" + monitor.data.id, { json: mon })
        .then(() => request.get(base_url + "/monitors/" + monitor.data.id))
        .then((res) => {
          res.data.relationships.servers.data.should.have.lengthOf(4);
        });
    });

    it("move relationships back to old monitor", function () {
      var mon = { data: { relationships: { servers: { data: null } } } };
      return request
        .patch(base_url + "/monitors/" + monitor.data.id, { json: mon })
        .then(() => request.get(base_url + "/monitors/" + monitor.data.id))
        .then((res) => {
          res.data.relationships.should.not.have.keys("servers");
        })
        .then(function () {
          mon.data.relationships.servers = {
            data: [
              { id: "server1", type: "servers" },
              { id: "server2", type: "servers" },
              { id: "server3", type: "servers" },
              { id: "server4", type: "servers" },
            ],
          };
          return request.patch(base_url + "/monitors/MariaDB-Monitor", { json: mon });
        })
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.servers.data.should.have.lengthOf(4);
        });
    });

    it("add service relationships to monitor", function () {
      var mon = {
        data: {
          relationships: {
            services: {
              data: [{ id: "RW-Split-Router", type: "services" }],
            },
          },
        },
      };

      return request
        .patch(base_url + "/services/RW-Split-Router", {
          json: { data: { relationships: { services: { data: null }, servers: { data: null } } } },
        })
        .then(() => request.patch(base_url + "/monitors/MariaDB-Monitor", { json: mon }))
        .then(() => request.get(base_url + "/services/RW-Split-Router"))
        .then((res) => {
          res.data.relationships.monitors.data[0].id.should.be.equal("MariaDB-Monitor");
          res.data.relationships.should.not.have.keys("servers");
          res.data.relationships.should.not.have.keys("services");
        });
    });

    it("remove service relationships from monitor", function () {
      var mon = { data: { relationships: { services: { data: null } } } };
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor", { json: mon })
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.should.not.have.keys("services");
        });
    });

    it("add relationships via `relationships` endpoint", function () {
      var old = {
        data: [
          { id: "server2", type: "servers" },
          { id: "server3", type: "servers" },
          { id: "server4", type: "servers" },
        ],
      };
      var created = { data: [{ id: "server1", type: "servers" }] };

      return request
        .patch(base_url + "/monitors/MariaDB-Monitor/relationships/servers", { json: old })
        .then(() =>
          request.patch(base_url + "/monitors/" + monitor.data.id + "/relationships/servers", {
            json: created,
          })
        )
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.servers.data.should.have.lengthOf(3);
        })
        .then(() => request.get(base_url + "/monitors/" + monitor.data.id))
        .then((res) => {
          res.data.relationships.servers.data.should.have
            .lengthOf(1)
            .that.deep.includes({ id: "server1", type: "servers" });
        });
    });

    it("bad request body with `relationships` endpoint should be rejected", function () {
      return request.patch(base_url + "/monitors/" + monitor.data.id + "/relationships/servers", {
        json: { servers: null },
      }).should.be.rejected;
    });

    it("remove relationships via `relationships` endpoint", function () {
      var old = {
        data: [
          { id: "server1", type: "servers" },
          { id: "server2", type: "servers" },
          { id: "server3", type: "servers" },
          { id: "server4", type: "servers" },
        ],
      };

      return request
        .patch(base_url + "/monitors/" + monitor.data.id + "/relationships/servers", { json: { data: null } })
        .then(() =>
          request.patch(base_url + "/monitors/MariaDB-Monitor/relationships/servers", { json: old })
        )
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.servers.data.should.have.lengthOf(4);
        })
        .then(() => request.get(base_url + "/monitors/" + monitor.data.id))
        .then((res) => {
          res.data.relationships.should.not.have.keys("servers");
        });
    });

    it("prevents partial configuration with malformed relationships", async function () {
      var mon = await request.get(base_url + "/monitors/MariaDB-Monitor");
      var old_interval = mon.data.attributes.parameters.monitor_interval
      mon.data.attributes.parameters.monitor_interval = old_interval * 2;
      mon.data.relationships.servers.data.push(mon.data.relationships.servers.data[0]);
      await request.patch(base_url + "/monitors/MariaDB-Monitor", { json: mon }).should.eventually.be.rejected
      var mon2 = await request.get(base_url + "/monitors/MariaDB-Monitor");
      mon2.data.attributes.parameters.monitor_interval.should.equal(old_interval);
    });

    it("add service relationships to monitor via `relationships` endpoint", function () {
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor/relationships/services", {
          json: { data: [{ id: "RW-Split-Router", type: "services" }] },
        })
        .then(() => request.get(base_url + "/services/RW-Split-Router"))
        .then((res) => res.data.relationships.monitors.data[0].id.should.be.equal("MariaDB-Monitor"));
    });

    it("remove service relationships from monitor via `relationships` endpoint", function () {
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor/relationships/services", { json: { data: null } })
        .then(() => request.get(base_url + "/monitors/MariaDB-Monitor"))
        .then((res) => {
          res.data.relationships.should.have.keys("servers");
        });
    });

    it("destroy created monitor", function () {
      return request.delete(base_url + "/monitors/" + monitor.data.id).should.be.fulfilled;
    });
  });

  describe("Regressions", function () {
    it("alter monitor should not remove servers", function () {
      var b = {
        data: {
          attributes: {
            parameters: {
              user: "test",
            },
          },
        },
      };
      return request
        .patch(base_url + "/monitors/MariaDB-Monitor", { json: b })
        .then(function () {
          return request.get(base_url + "/monitors/MariaDB-Monitor");
        })
        .then(function (mon) {
          mon.data.relationships.servers.data.should.not.be.empty;
        });
    });
  });

  after(restartMaxScale);
});
