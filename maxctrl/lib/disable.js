/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
const { maxctrl, error, updateValue, helpMsg } = require("./common.js");

const log_levels = ["debug", "info", "notice", "warning"];

exports.command = "disable <command>";
exports.desc = "Disable functionality";
exports.handler = function () {};
exports.builder = function (yargs) {
  yargs
    .command(
      "log-priority <log>",
      "Disable log priority [warning|notice|info|debug]",
      function (yargs) {
        return yargs
          .epilog("The `debug` log priority is only available for debug builds of MaxScale.")
          .usage("Usage: disable log-priority <log>");
      },
      function (argv) {
        if (log_levels.indexOf(argv.log) != -1) {
          maxctrl(argv, function (host) {
            return updateValue(host, "maxscale/logs", "data.attributes.parameters.log_" + argv.log, false);
          });
        } else {
          maxctrl(argv, function () {
            return error("Invalid log priority: " + argv.log);
          });
        }
      }
    )
    .usage("Usage: disable <command>")
    .help()
    .wrap(null)
    .demandCommand(1, helpMsg);
};
