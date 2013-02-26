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
import java.util.Properties;

import org.apache.whirr.Cluster;
// import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRMRMasterClusterActionHandler extends MapRClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(MapRMRMasterClusterActionHandler.class);
  
  public static final String ROLE = "mapr-mr-master";
  
  @Override
  public String getRole() {
    return ROLE;
  }
  

/* The "doBeforeConfiguration" and "afterConfiguration" operations
 * should be the effective merging of the functions from CLDB ActionHandler 
 * and Zookeeper ActionHandler, since the "master" node handles both 
 * services.
 *
 * Remember : MapR clusters will likely have many "master" nodes, so
 *            we need to be careful about the firewall configurations.
 */
  @Override
  protected void doBeforeConfigure(ClusterActionEvent event) 
    throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Begin configuration of {} role {}", clusterSpec.getClusterName(), getRole());

    if (cluster.getInstancesMatching(RolePredicates.role(
           MapRMRMasterClusterActionHandler.ROLE)) != null) {
      event.getFirewallManager().addRules(
        Rule.create()
          .destination(RolePredicates.role(MapRMRMasterClusterActionHandler.ROLE))
          .ports(MapRCluster.CLDB_PORT,MapRCluster.WEB_PORT,MapRCluster.NFS_PORT,MapRCluster.RPCBIND_PORT));
    }
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) 
    throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    LOG.info("Completed configuration of {} role {}", clusterSpec.getClusterName(), getRole());
    InetAddress cldbPublicAddress = MapRCluster.getCLDBPublicAddress(cluster);
    InetAddress jobtrackerPublicAddress = MapRCluster.getJobTrackerPublicAddress(cluster);

    Properties config = createClientSideProperties(clusterSpec, cldbPublicAddress, jobtrackerPublicAddress);
    createClientSideHadoopSiteFile(clusterSpec, config);
    createProxyScript(clusterSpec, cluster);
    Properties combined = new Properties();
    combined.putAll(cluster.getConfiguration());
    combined.putAll(config);
    event.setCluster(new Cluster(cluster.getInstances(), combined));
  }
}
