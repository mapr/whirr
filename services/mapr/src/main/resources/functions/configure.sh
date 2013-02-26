#!/bin/bash
set +u
shopt -s xpg_echo
shopt -s expand_aliases

PROMPT_COMMAND='echo -ne \"\033]0;configure-mapr-cldb_mapr-zookeeper\007\"'
export PATH=/usr/ucb/bin:/bin:/sbin:/usr/bin:/usr/sbin

export INSTANCE_NAME='configure-mapr-cldb_mapr-zookeeper'
export CLUSTER_NAME='mrc'
export CLOUD_PROVIDER='aws-ec2'
export ROLES='mapr-cldb,mapr-zookeeper'
export PUBLIC_IP='54.241.65.21'
export PRIVATE_IP='10.196.32.211'
export PUBLIC_HOST_NAME='ec2-54-241-65-21.us-west-1.compute.amazonaws.com'
export PRIVATE_HOST_NAME='10.196.32.211'
export INSTANCE_NAME='configure-mapr-cldb_mapr-zookeeper'
export INSTANCE_HOME='/tmp/configure-mapr-cldb_mapr-zookeeper'
export LOG_DIR='/tmp/configure-mapr-cldb_mapr-zookeeper'
function abort {
   echo "aborting: $@" 1>&2
   exit 1
}
#!/bin/bash

BASE_ROLES=${INSTANCE_NAME#bootstrap-}
MAPR_ROLES=${BASE_ROLES/_/,}
MAPR_HOME=${MAPR_HOME:-/opt/mapr}

function remove_from_fstab() {
	mnt=${1}
	[ -z "${mnt}" ] && return

	escapedMnt=`echo "$mnt" | sed "s#/#\\/#g"`
	sed -i.mapr_save '/$escapedMnt/d' /etc/fstab
	if [ $? -eq 0 ] ; then
		echo "[ERROR]: failed to remove $mnt from /etc/fstab"
	fi
}

function unmount_unused() {
	fsToUnmount=${1:-}
	
  	for fs in `echo ${fsToUnmount/,/ /}`
	do
		fuser -s $fs 2> /dev/null
		if [ $? -ne 0 ] ; then
			umount $fs
			[ $? -eq 0 ] && remove_from_fstab $fs
		fi
	done
}

function identify_unused_disks() {
	echo "Searching for unused disks"

	disks=""
	for dsk in `fdisk -l 2> /dev/null  | grep -e "^Disk .* bytes$" | awk '{print $2}'`
	do
		dev=${dsk%:}

		mounted=0; isSwap=0; isLVM=0 ;
		mount | grep -q -w $dev
		[ $? -eq 0 ] && mounted=1

		swapon -s | grep -q -w $dev
		[ $? -eq 0 ] && isSwap=1

		if which pvdisplay &> /dev/null; then
			pvdisplay $dev 2> /dev/null
			[ $? -eq 0 ] && isLVM=1
		fi
		if [ $mounted -eq 0  -a  $isSwap -eq 0  -a $isLVM -eq 0 ] ; then
			disks="$disks $dev"
		fi
	done
	
	echo "unused disks: $disks"
	MAPR_DISKS="$disks"
	export MAPR_DISKS
}

function configure_mapr() {
  local OPTIND
  local OPTARG

  if [ "$CONFIGURE_MAPR_DONE" == "1" ]; then
    echo "Mapr is already configured on this node."
    return;
  fi

# Options
#	n: cldb nodes (comma separated)
#	z: Zookeeper nodes (comma separated)
#	u: unmount these before disk_setup
#
  while getopts "u:n:z:c:" OPTION; do
    case $OPTION in
    u)
      unMountPaths="$OPTARG"
      ;;
    n)
      cldbNodes="$OPTARG"
      ;;
    z)
      zkNodes="$OPTARG"
      ;;
    esac
  done

  echo "configuring MapR services for cluster $CLUSTER_NAME"
  echo "cldbNodes=$cldbNodes   zkNodes=$zkNodes"

  [ -n "${CLUSTER_NAME:-}" ] && carg="-N $CLUSTER_NAME"
  $MAPR_HOME/server/configure.sh -C $cldbNodes -Z $zkNodes ${carg:-} --isvm

  [ -n "${unMountPaths:-}" ] && unmount_unused $unMountPaths

  if [ -f ${MAPR_HOME}/roles/fileserver ] ; then
  	identify_unused_disks
  	echo "MAPR_DISKS=$MAPR_DISKS"

	if [ -n "${MAPR_DISKS}" ] ; then
		diskfile=/tmp/MapR.disks
		rm -f $diskfile
		for d in $MAPR_DISKS ; do echo $d ; done >> $diskfile
		$MAPR_HOME/server/disksetup -F $diskfile
		if [ $? -ne 0 ] ; then
			echo "[ERROR]: disksetup failed"
		fi
	else
		echo "[ERROR]: MapR fileserver package is installed,"
		echo "         but no unused disks exist to be utilized."
	fi
  fi

  CONFIGURE_MAPR_DONE=1
}


#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function retry_helpers() {
  echo "This function does nothing. It just needs to exist so Statements.call(\"retry_helpers\") doesn't call something which doesn't exist"
}

function retry() {
  tries=$1
  interval=$2
  expected_exit_code=$3
  shift 3

  while [ "$tries" -gt 0 ]; do
    $@
    last_exit_code=$?

    if [ "$last_exit_code" -eq "$expected_exit_code" ]; then
      break
    fi

    tries=$((tries-1))
    if [ "$tries" -gt 0 ]; then
      sleep $interval
    fi
  done
  # Ugly hack to avoid substitution (re_turn -> exit)
  "re""turn" $last_exit_code
}

function retry_apt_get() {
  retry 5 5 0 apt-get -o APT::Acquire::Retries=5 $@
}

function retry_yum() {
  retry 5 5 0 yum $@
}


# Need to figure out why this is called twice
configure_mapr   -u /mnt,/foobar -n 10.196.32.211 -z 10.196.32.211  || exit 1
configure_mapr mapr-cldb,mapr-zookeeper -c aws-ec2 || exit 1

exit $?

