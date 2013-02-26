#!/bin/bash
#
#	INSTANCE_NAME is set as part of the jclouds framework
#	into which this script is integrated
#		There should be a better way to get the other settings
#		from the framework to this script ... but I haven't figured it out
#
#	Looks like we need to use the silly "retry_helpers" within
#	Whirr to make sure this works.
#		TBD : write local wrappers to handle this in case we want to 
#			  use the script independent of Whirr
#
BASE_ROLES=${INSTANCE_NAME#bootstrap-}
MAPR_ROLES=${BASE_ROLES//_/,}
MAPR_VERSION=${MAPR_VERSION:-2.1.1}
MAPR_HOME=${MAPR_HOME:-/opt/mapr}
MAPR_ENV_FILE=$MAPR_HOME/conf/env.sh

MAPR_USER=${MAPR_USER:-mapr}
MAPR_PASSWD=${MAPR_PASSWD:-MapR}

function add_mapr_user() {
	id $MAPR_USER &> /dev/null
	[ $? -eq 0 ] && return ;

	useradd -u 2000 -c "MapR" -m -s /bin/bash $MAPR_USER 2> /dev/null
	if [ $? -ne 0 ] ; then
			# Assume failure was dup uid; try with system-specified id
		useradd -c "MapR" -m -s /bin/bash $MAPR_USER
	fi

	if [ $? -ne 0 ] ; then
		echo "Failed to create new user $MAPR_USER"
	else
		passwd $MAPR_USER << passwdEOF
$MAPR_PASSWD
$MAPR_PASSWD
passwdEOF

	fi

		# Extract the ssh-key from the JCloud-created user
		# and save it as our own.  This has the desired effect
		# of making all mapr users in the cluster interchangeable
		#	(though it is a security hole since the key is non-unique)
	if [ -n "${NEW_USER:-}"  -a   ${NEW_USER} != ${MAPR_USER} ] ; then
		new_userdir=`eval "echo ~${NEW_USER}"`
		mapr_userdir=`eval "echo ~${MAPR_USER}"`
		if [ -f ${new_userdir}/.ssh/id_rsa ] ; then
			cp -r  ${new_userdir}/.ssh  ${mapr_userdir}
			chmod 700 ${mapr_userdir}/.ssh
			chown -R ${MAPR_USER}:`id -gn ${MAPR_USER}` ${mapr_userdir}/.ssh
		fi
	fi

		# TO BE DONE
}

# I saw REAL problems here with Amazon Images ... often resulting
# in the failure to install Java.  I tried several varians, with 
# NO success.  In particular, adding "apt-get clean", which I 
# thought would help, had no usefulness.
# 
function update_os_deb() {
	retry_apt_get update
	retry_apt_get upgrade -y --force-yes
}

function update_os_rpm() {
	retry_yum make-cache
	retry_yum update -y
}

function update_os() {
  echo "Installing OS security updates"

  if which dpkg &> /dev/null; then
    update_os_deb
  elif which rpm &> /dev/null; then
    update_os_rpm
  fi
}

function setup_mapr_repo_deb() {
    MAPR_REPO_FILE=/etc/apt/sources.list.d/mapr.list
    MAPR_PKG="http://package.mapr.com/releases/v${MAPR_VERSION}/ubuntu"
    MAPR_ECO="http://package.mapr.com/releases/ecosystem/ubuntu"

    [ -f $MAPR_REPO_FILE ] && return ;

    echo Setting up repos in $MAPR_REPO_FILE
    cat > $MAPR_REPO_FILE << EOF_ubuntu
deb $MAPR_PKG mapr optional
deb $MAPR_ECO binary/
EOF_ubuntu
	
    retry_apt_get update
}

function setup_mapr_repo_rpm() {
    MAPR_REPO_FILE=/etc/yum.repos.d/mapr.repo
    MAPR_PKG="http://package.mapr.com/releases/v${MAPR_VERSION}/redhat"
    MAPR_ECO="http://package.mapr.com/releases/ecosystem/redhat"

    [ -f $MAPR_REPO_FILE ] && return ;

    echo Setting up repos in $MAPR_REPO_FILE
    cat > $MAPR_REPO_FILE << EOF_redhat
[MapR]
name=MapR Version $MAPR_VERSION media
baseurl=$MAPR_PKG
enabled=1
gpgcheck=0
protected=1

[MapR_ecosystem]
name=MapR Ecosystem Components
baseurl=$MAPR_ECO
enabled=1
gpgcheck=0
protected=1
EOF_redhat

        # Metrics requires some packages in EPEL ... so we'll
        # add those repositories as well
        #   NOTE: this target will change FREQUENTLY !!!
    EPEL_RPM=/tmp/epel.rpm
    CVER=`lsb_release -r | awk '{print $2}'`
    if [ ${CVER%.*} -eq 5 ] ; then
        EPEL_LOC="epel/5/x86_64/epel-release-5-4.noarch.rpm"
    else
        EPEL_LOC="epel/6/x86_64/epel-release-6-8.noarch.rpm"
    fi

    wget -O $EPEL_RPM http://download.fedoraproject.org/pub/$EPEL_LOC
    [ $? -eq 0 ] && rpm --quiet -i $EPEL_RPM

    retry_yum makecache
}

function setup_mapr_repo() {
  if which dpkg &> /dev/null; then
    setup_mapr_repo_deb
  elif which rpm &> /dev/null; then
    setup_mapr_repo_rpm
  fi
}

function install_mapr_packages() {
  echo "Installing MapR packages for $MAPR_ROLES"

  pkgs=""
  for role in `echo ${MAPR_ROLES//,/ }`
  do
	case "$role" in
		mapr-cldb|\
		mapr-client|\
		mapr-fileserver|\
		mapr-jobtracker|\
		mapr-metrics|\
		mapr-nfs|\
		mapr-tasktracker|\
		mapr-webserver|\
		mapr-zookeeper )
			pkgs="$pkgs $role"
			;;
		mapr-simple|\
		mapr-mr-master)
			pkgs="mapr-cldb mapr-zookeeper mapr-jobtracker"
			pkgs="$pkgs mapr-webserver mapr-nfs"
			pkgs="$pkgs mapr-fileserver mapr-tasktracker"
			;;
		mapr-mr-slave)
			pkgs="mapr-fileserver mapr-tasktracker"
			;;
		*) 
			echo "[WARNING]: Unrecognized role ($role)"
			;;
	esac
  done

  echo "	pkgs=$pkgs"

  if [ -z "${pkgs}" ] ; then
	echo "[ERROR]: no packages to install"
	return
  fi

  if which dpkg &> /dev/null; then
    retry_apt_get install -y --force-yes $pkgs
  elif which rpm &> /dev/null; then
    retry_yum install -y --force-yes $pkgs
  else
   echo "[ERROR]: could not find dpkg or rpm executable in path" 1>&2
  fi
}

# Helper utility to update ENV settings in env.sh.
# Function is replicated in the spinup-mapr-node.sh script.
# Function WILL NOT override existing settings ... it looks
# for the default "#export <var>=" syntax and substitutes the new value

update_env_sh()
{
    [ -z "${1:-}" ] && return 1
    [ -z "${2:-}" ] && return 1

    AWK_FILE=/tmp/ues$$.awk
    cat > $AWK_FILE << EOF_ues
/^#export ${1}=/ {
    getline
    print "export ${1}=$2"
}
{ print }
EOF_ues

    cp -p $MAPR_ENV_FILE ${MAPR_ENV_FILE}.imager_save
    awk -f $AWK_FILE ${MAPR_ENV_FILE} > ${MAPR_ENV_FILE}.new
    [ $? -eq 0 ] && mv -f ${MAPR_ENV_FILE}.new ${MAPR_ENV_FILE}
}


#
#	Input to this function (TBD ... for now, we grab values from our env)
#		MapR version (simple string, eg 2.0.1, 2.1.1)
#		roles for this node (comma-separated list)
#

function install_mapr() {
  local OPTIND
  local OPTARG

  if [ "$INSTALL_MAPR_DONE" == "1" ]; then
    echo "Mapr is already installed on this node."
    return;
  fi
  
  echo "Entering install_mapr()"

  while [ -n "${1:-}" ] ; do
  	echo $1
	shift
  done

  update_os

  add_mapr_user
  setup_mapr_repo
  install_mapr_packages

  echo "Updating configuration in $MAPR_ENV_FILE"
  update_env_sh MAPR_HOME $MAPR_HOME
  update_env_sh JAVA_HOME $JAVA_HOME

  INSTALL_MAPR_DONE=1
  echo "Exiting install_mapr()"
}

