#!/bin/bash
set +u
shopt -s xpg_echo
shopt -s expand_aliases

PROMPT_COMMAND='echo -ne \"\033]0;bootstrap-mapr-cldb_mapr-zookeeper\007\"'
export PATH=/usr/ucb/bin:/bin:/sbin:/usr/bin:/usr/sbin

export INSTANCE_NAME='bootstrap-mapr-cldb_mapr-zookeeper'
export CLUSTER_NAME='mrc'
export CLOUD_PROVIDER='aws-ec2'
export NEW_USER='dtucker'
export DEFAULT_HOME='/home/users'
export INSTANCE_NAME='bootstrap-mapr-cldb_mapr-zookeeper'
export INSTANCE_HOME='/tmp/bootstrap-mapr-cldb_mapr-zookeeper'
export LOG_DIR='/tmp/bootstrap-mapr-cldb_mapr-zookeeper'
function abort {
   echo "aborting: $@" 1>&2
   exit 1
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
function install_openjdk_deb() {
  retry_apt_get update
  retry_apt_get -y install openjdk-6-jdk
  
  # Try to set JAVA_HOME in a number of commonly used locations
  # Lifting JAVA_HOME detection from jclouds
  if [ -z "$JAVA_HOME" ]; then
      for CANDIDATE in `ls -d /usr/lib/jvm/java-1.6.0-openjdk-* /usr/lib/jvm/java-6-openjdk-* /usr/lib/jvm/java-6-openjdk 2>&-`; do
          if [ -n "$CANDIDATE" -a -x "$CANDIDATE/bin/java" ]; then
              export JAVA_HOME=$CANDIDATE
              break
          fi
      done
  fi

  if [ -f /etc/profile ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/profile
  fi
  if [ -f /etc/bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/bashrc
  fi
  if [ -f ~root/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> ~root/.bashrc
  fi
  if [ -f /etc/skel/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/skel/.bashrc
  fi
  if [ -f "$DEFAULT_HOME/$NEW_USER" ]; then
    echo export JAVA_HOME=$JAVA_HOME >> $DEFAULT_HOME/$NEW_USER
  fi

  update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
  update-alternatives --set java $JAVA_HOME/bin/java
  java -version
}

function install_openjdk_rpm() {
  retry_yum -y install java-1.6.0-openjdk-devel
  
  # Try to set JAVA_HOME in a number of commonly used locations
  # Lifting JAVA_HOME detection from jclouds
  if [ -z "$JAVA_HOME" ]; then
      for CANDIDATE in `ls -d /usr/lib/jvm/java-1.6.0-openjdk-* /usr/lib/jvm/java-6-openjdk-* /usr/lib/jvm/java-6-openjdk 2>&-`; do
          if [ -n "$CANDIDATE" -a -x "$CANDIDATE/bin/java" ]; then
              export JAVA_HOME=$CANDIDATE
              break
          fi
      done
  fi
  if [ -f /etc/profile ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/profile
  fi
  if [ -f /etc/bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/bashrc
  fi
  if [ -f ~root/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> ~root/.bashrc
  fi
  if [ -f /etc/skel/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/skel/.bashrc
  fi
  if [ -f "$DEFAULT_HOME/$NEW_USER" ]; then
    echo export JAVA_HOME=$JAVA_HOME >> $DEFAULT_HOME/$NEW_USER
  fi

  alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
  alternatives --set java $JAVA_HOME/bin/java
  java -version
}

function install_openjdk() {
  if which dpkg &> /dev/null; then
    install_openjdk_deb
  elif which rpm &> /dev/null; then
    install_openjdk_rpm
  fi
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
function configure_hostnames() {
  local OPTIND
  local OPTARG

  if [ ! -z $AUTO_HOSTNAME_SUFFIX ]; then
      PUBLIC_IP=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
      HOSTNAME=${AUTO_HOSTNAME_PREFIX}`echo $PUBLIC_IP | tr . -`${AUTO_HOSTNAME_SUFFIX}
      if [ -f /etc/hostname ]; then
          echo $HOSTNAME > /etc/hostname
      fi
      if [ -f /etc/sysconfig/network ]; then
          sed -i -e "s/HOSTNAME=.*/HOSTNAME=$HOSTNAME/" /etc/sysconfig/network
      fi
      sed -i -e "s/$PUBLIC_IP.*/$PUBLIC_IP $HOSTNAME/" /etc/hosts
      set +e
      if [ -f /etc/init.d/hostname ]; then
          /etc/init.d/hostname restart
      else
          hostname $HOSTNAME
      fi
      set -e
      sleep 2
      hostname
  fi
}
#!/bin/bash

BASE_ROLES=${INSTANCE_NAME#bootstrap-}
MAPR_ROLES=${BASE_ROLES/_/,}
MAPR_VERSION=2.1.1

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
	
    apt-get update
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

    yum makecache
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
  for role in `echo ${MAPR_ROLES/,/ /}`
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
		mapr-m3-master)
			pkgs="mapr-cldb mapr-zookeeper mapr-jobtracker"
			pkgs="$pkgs mapr-webserver"
			pkgs="$pkgs mapr-fileserver mapr-tasktracker"
			;;
		mapr-m3-slave)
			pkgs="$pkgs mapr-fileserver mapr-tasktracker"
			;;
		mapr-m5-master)
			pkgs="mapr-cldb mapr-zookeeper mapr-jobtracker"
			pkgs="$pkgs mapr-webserver"
			pkgs="$pkgs mapr-fileserver mapr-tasktracker"
			;;
		mapr-m5-slave)
			pkgs="$pkgs mapr-fileserver mapr-tasktracker"
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
    apt-get install -y --force-yes $pkgs
  elif which rpm &> /dev/null; then
    yum install -y --force-yes $pkgs
  else
   echo "[ERROR]: could not find dpkg or rpm executable in path" 1>&2
  fi
}

install_mapr() {
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

  setup_mapr_repo
  install_mapr_packages

  INSTALL_HADOOP_DONE=1
  echo "Exiting install_mapr()"
}


cd $INSTANCE_HOME
rm -f $INSTANCE_HOME/rc
trap 'echo $?>$INSTANCE_HOME/rc' 0 1 2 3 15
USER_HOME=$DEFAULT_HOME/$NEW_USER

# Initialize user and various keys

# install_openjdk || exit 1

# retry_helpers || exit 1

# configure_hostnames || exit 1

install_mapr || exit 1

exit $?

