/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.mapr;

import java.io.IOException;
import java.net.InetAddress;
//import java.util.List;
//import java.util.Map;
import java.util.Set;
//import java.util.Properties;

import org.apache.whirr.Cluster;
// import org.apache.whirr.ClusterSpec;
// import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.RolePredicates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MapRCluster {
  private static final Logger LOG = LoggerFactory.getLogger(MapRCluster.class);

  public static final int CLDB_PORT          = 7222;
  public static final int CLDB_WEB_UI_PORT   = 7221;
  public static final int CLDB_JMX_PORT      = 7220;

  public static final int JOBTRACKER_PORT = 9001;
  public static final int JOBTRACKER_WEB_UI_PORT = 50030;

  public static final int NFS_PORT        = 2049;
  public static final int RPCBIND_PORT    = 111;

  public static final int WEB_PORT        = 8443;
  public static final int FILESERVER_PORT = 5660;

  public static final int TASKTRACKER_WEB_UI_PORT     = 50060;

  public static final int HBASE_MASTER_WEB_PORT  = 60010;
  public static final int HBASE_REGIONSERVER_WEB_PORT = 60030;
 
  public static final int ZOOKEEPER_PORT = 5181;
  public static final int ZOOKEEPER_FTL_PORT = 2888;
  public static final int ZOOKEEPER_ELECTION_PORT = 3888;

    /* This code should be smarter and return ALL addresses
       for the CLDB nodes.  For now, we'll return the "first"
       IP address (lowest private addr), and let it serve as the proxy.
     */
  public static InetAddress getCLDBPublicAddress(Cluster cluster) 
    throws IOException {
    InetAddress cldbPublicAddress = null;
    long cldbIP = Long.MAX_VALUE;

    Set<Cluster.Instance> cldbInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRCLDBClusterActionHandler.ROLE));

    if (cldbInstances == null  ||  cldbInstances.size() == 0) {
        cldbInstances = 
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        cldbInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

    for (Cluster.Instance inst: cldbInstances) {
        InetAddress pAddr = inst.getPrivateAddress();
        long ip = MapRCluster.ipToLong(pAddr.getAddress());
        if (ip < cldbIP) {
            cldbIP = ip;
            cldbPublicAddress = inst.getPublicAddress();
        }
    }
    return cldbPublicAddress ;
  }

  public static InetAddress getCLDBPrivateAddress(Cluster cluster) 
  throws IOException {
    InetAddress cldbPrivateAddress = null;
    long cldbIP = Long.MAX_VALUE;

    Set<Cluster.Instance> cldbInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRCLDBClusterActionHandler.ROLE));

    if (cldbInstances == null  ||  cldbInstances.size() == 0) {
        cldbInstances = 
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        cldbInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

    for (Cluster.Instance inst: cldbInstances) {
        InetAddress pAddr = inst.getPrivateAddress();
        long ip = MapRCluster.ipToLong(pAddr.getAddress());
        if (ip < cldbIP) {
            cldbIP = ip;
            cldbPrivateAddress = inst.getPrivateAddress();
        }
    }
    return cldbPrivateAddress ;
  }

    /* This code should be smarter and return the address of
       the active JobTracker (since there may be backup job trackers running)
     */
  public static InetAddress getJobTrackerPublicAddress(Cluster cluster) 
  throws IOException {
    InetAddress jtPublicAddress = null;
    long jtIP = Long.MAX_VALUE;

    Set<Cluster.Instance> jtInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRJobTrackerClusterActionHandler.ROLE));

    if (jtInstances == null  ||  jtInstances.size() == 0) {
        jtInstances = 
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        jtInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

    for (Cluster.Instance inst: jtInstances) {
        InetAddress pAddr = inst.getPrivateAddress();
        long ip = MapRCluster.ipToLong(pAddr.getAddress());
        if (ip < jtIP) {
            jtIP = ip;
            jtPublicAddress = inst.getPublicAddress();
        }
    }
    return jtPublicAddress ;
  }

  public static InetAddress getJobTrackerPrivateAddress(Cluster cluster) 
  throws IOException {
    InetAddress jtPrivateAddress = null;
    long jtIP = Long.MAX_VALUE;

    Set<Cluster.Instance> jtInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRJobTrackerClusterActionHandler.ROLE));

    if (jtInstances == null  ||  jtInstances.size() == 0) {
        jtInstances = 
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        jtInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

    for (Cluster.Instance inst: jtInstances) {
        InetAddress pAddr = inst.getPrivateAddress();
        long ip = MapRCluster.ipToLong(pAddr.getAddress());
        if (ip < jtIP) {
            jtIP = ip;
            jtPrivateAddress = inst.getPrivateAddress();
        }
    }
    return jtPrivateAddress ;
  }

  public static long ipToLong (byte[] address) {
    long ip = 0;
    for (int i=0; i<address.length; ++i) {
        ip <<= 8;
        ip |= (address[i] & 0xFF);
    }

    return ip;
  }


}
