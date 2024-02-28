#!/bin/bash
#
# Copyright (c) 2016 MariaDB Corporation Ab
# Copyright (c) 2023 MariaDB plc, Finnish Branch
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file and at www.mariadb.com/bsl11.
#
# Change Date: 2028-02-27
#
# On the date above, in accordance with the Business Source License, use
# of this software will be governed by version 2 or later of the General
# Public License.
#

script=`basename "$0"`

source=$src_dir/masking/$1/masking_rules.json
target=${maxscale_000_whoami}@${maxscale_000_network}:/home/${maxscale_000_whoami}/masking_rules.json

if [ ${maxscale_000_network} != "127.0.0.1" ] ; then
        scp -i $maxscale_000_keyfile -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $source $target
        ssh -i $maxscale_000_keyfile -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${maxscale_000_whoami}@${maxscale_000_network} "sudo chmod o+r /home/${maxscale_000_whoami}/masking_rules.json"
else
        cp $source /home/${maxscale_000_whoami}/masking_rules.json
fi

if [ $? -ne 0 ]
then
    echo "error: Could not copy rules file to maxscale host."
    exit 1
fi

echo $source copied to $target, restarting maxscale

ssh  -i $maxscale_000_keyfile -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${maxscale_000_whoami}@${maxscale_000_network} 'sudo systemctl restart maxscale'

test_dir=`pwd`

logdir=log_$1
[ -d $logdir ] && rm -r $logdir
mkdir $logdir || exit 1

# [Read Connection Listener Master] in cnf/maxscale.maxscale.cnf.template.$1
port=4008

dir="$src_dir/masking/$1"

user=skysql
test_name=masking_user
mysqltest --no-defaults \
          --host=${maxscale_000_network} --port=$port \
          --user=$user --password=$maxscale_password \
          --logdir=$logdir \
          --test-file=$dir/t/$test_name.test \
          --result-file=$dir/r/"$test_name"_"$user".result \
          --silent
if [ $? -eq 0 ]
then
    echo " OK"
else
    echo " FAILED"
    res=1
fi

user=maxskysql
test_name=masking_user
mysqltest --no-defaults \
          --host=${maxscale_000_network} --port=$port \
          --user=$user --password=$maxscale_password \
          --logdir=$logdir \
          --test-file=$dir/t/$test_name.test \
          --result-file=$dir/r/"$test_name"_"$user".result \
          --silent
if [ $? -eq 0 ]
then
    echo " OK"
else
    echo " FAILED"
    res=1
fi

exit $res
