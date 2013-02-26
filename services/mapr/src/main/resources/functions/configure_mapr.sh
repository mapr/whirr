#!/bin/bash

#
#	INSTANCE_NAME is set as part of the jclouds framework
#	into which this script is integrated
#
BASE_ROLES=${INSTANCE_NAME#bootstrap-}
MAPR_ROLES=${BASE_ROLES//_/,}
MAPR_HOME=${MAPR_HOME:-/opt/mapr}
MAPR_USER=${MAPR_USER:-mapr}
MAPR_PASSWD=${MAPR_PASSWD:-MapR}

function remove_from_fstab() {
	mnt=${1}
	[ -z "${mnt}" ] && return

	FSTAB=/etc/fstab
	[ ! -w $FSTAB ] && return

	sedOpt="/"`echo "$mnt" | sed -e 's/\//\\\\\//g'`"/d"
	sed -i.mapr_save $sedOpt $FSTAB
	if [ $? -ne 0 ] ; then
		echo "[ERROR]: failed to remove $mnt from $FSTAB"
	fi
}

function unmount_unused() {
	fsToUnmount=${1:-}
	
  	for fs in `echo ${fsToUnmount//,/ }`
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

configure_mapr_fileserver() {
  if [ -f ${MAPR_HOME}/roles/fileserver ] ; then
  	echo "Configuring disks and fileserver services"
  else
  	echo "No MapR fileserver services enabled"
  	return
  fi

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
}

configure_mapr_nfs() {
  if [ -f ${MAPR_HOME}/roles/nfsserver ] ; then
  	echo "Configuring MapR nfs services"
  else
  	echo "MapR-nfs not configured on this node, and no target NFS servers specified"
  	return
  fi

  MAPR_FSMOUNT=/mapr
  MAPR_FSTAB=$MAPR_HOME/conf/mapr_fstab
  SYSTEM_FSTAB=/etc/fstab
  echo $MAPR_ROLES | grep -q nfs
  if [ $? -eq 0 ] ; then
    MAPR_NFS_SERVER=localhost
    MAPR_NFS_OPTIONS="hard,intr,nolock"
  else
    MAPR_NFS_OPTIONS="hard,intr"
  fi

# So the system either has the NFS service or is a client to a MapR nfs server
if [ -n "${MAPR_NFS_SERVER}" ] ; then
    echo "Mouting ${MAPR_NFS_SERVER}:/mapr/$CLUSTER_NAME to $MAPR_FSMOUNT" >> $LOG
    mkdir $MAPR_FSMOUNT

    if [ $MAPR_NFS_SERVER = "localhost" ] ; then
        echo "${MAPR_NFS_SERVER}:/mapr/$CLUSTER_NAME $MAPR_FSMOUNT   $MAPR_NFS_OPTIONS" >> $MAPR_FSTAB

        $MAPR_HOME/initscripts/mapr-nfsserver restart
    else
        echo "${MAPR_NFS_SERVER}:/mapr/$CLUSTER_NAME $MAPR_FSMOUNT   nfs $MAPR_NFS_OPTIONS   0   0" >> $SYSTEM_FSTAB
        mount $MAPR_FSMOUNT
    fi
fi


}

function configure_mapr() {
  local OPTIND
  local OPTARG

  echo "Entering configure_mapr()"

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
  echo "  MAPR_USER=$MAPR_USER"
  echo "  cldbNodes=$cldbNodes   zkNodes=$zkNodes"

	# BE WARNED ... letting configure.sh create the MapR user
	# is NOT a good plan.
	#	1. User is not given proper sudo privileges
	#	2. No home directory is created
	#	3. Some ownerships within $MAPR_HOME are not initialized properly
	#		(specifically the zkdata and zookeeper directories)
	#
	# Solution: create mapr user in install_mapr.sh
	#
  id $MAPR_USER &> /dev/null
  [ $? -ne 0 ] && uOpt="--create-user -u $MAPR_USER"

  [ -n "${CLUSTER_NAME:-}" ] && cOpt="-N $CLUSTER_NAME"
  $MAPR_HOME/server/configure.sh -C $cldbNodes -Z $zkNodes \
  	${uOpt:-} ${cOpt:-} --isvm

	# Set a password for the MapR user just in case.
  if [ -n "${uOpt}" ] ; then
    passwd $MAPR_USER << pEOF
$MAPR_PASSWD
$MAPR_PASSWD
pEOF
  fi

  [ -n "${unMountPaths:-}" ] && unmount_unused $unMountPaths

# At this point, we should configure the services that
# need to run.  It's a toss-up whether to look for the
# proper installation of the role (probably safer) or the
# specification in our input string.  Checking the MAPR_HOME
# directory is a little more portable to different cloud 
# environments, so I'll stick with that.

  configure_mapr_fileserver

	# Start up services; can look in $MAPR_HOME/roles or /etc/init.d 
  if [ -f ${MAPR_HOME}/roles/zookeeper ] ; then
		# Saw a bug when user was created with $MAPR_HOME/server/configure.sh
		# ... $MAPR_HOME/zkdata has incorrect permissions; fix it here
	chown $MAPR_USER $MAPR_HOME/zkdata
	chown $MAPR_USER $MAPR_HOME/zookeeper

  	service mapr-zookeeper start
  fi

	# We could be smart here ... delay the startup of non-CLDB nodes 
	# to make the cluster launch a little smoother
  if [ -f ${MAPR_HOME}/roles/cldb ] ; then
  	service mapr-warden start
  else
#	sleep 10
  	service mapr-warden start
  fi

  CONFIGURE_MAPR_DONE=1
  echo "Exiting configure_mapr()"
}

