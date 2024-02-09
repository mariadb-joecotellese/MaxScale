#!/bin/bash
#
# Copyright (c) 2016 MariaDB Corporation Ab
# Copyright (c) 2023 MariaDB plc, Finnish Branch
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file and at www.mariadb.com/bsl11.
#
# Change Date: 2028-01-30
#
# On the date above, in accordance with the Business Source License, use
# of this software will be governed by version 2 or later of the General
# Public License.
#

###
## @file bug567.sh Regression case for the bug "Crash if files from /dev/shm/ removed"
## - try to remove everythign from /dev/shm/$maxscale_pid
## check if Maxscale is alive

export ssl_options="--ssl-cert=$src_dir/ssl-cert/client-cert.pem --ssl-key=$src_dir/ssl-cert/client-key.pem"

#pid=`ssh -i $maxscale_sshkey -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${maxscale_000_whoami}@${maxscale_000_network} "pgrep maxscale"`
#echo "Maxscale pid is $pid"
echo "removing log directory from /dev/shm/"
if [ ${maxscale_000_network} != "127.0.0.1" ] ; then
	ssh -i ${maxscale_000_keyfile} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${maxscale_000_whoami}@${maxscale_000_network} "sudo rm -rf /dev/shm/maxscale/*"
else
	sudo rm -rf /dev/shm/maxscale/*
fi
sleep 1
echo "checking if Maxscale is alive"
echo "show databases;" | mysql -u$node_user -p$node_password -h ${maxscale_000_network} -P 4006 $ssl_options
res=$?

exit $res

