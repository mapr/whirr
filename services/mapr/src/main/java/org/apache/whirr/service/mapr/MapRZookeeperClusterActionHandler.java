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
import java.util.Set;

import com.google.common.collect.Iterables;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRZookeeperClusterActionHandler extends MapRClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(MapRZookeeperClusterActionHandler.class);
  
  public static final String ROLE = "mapr-zookeeper";
  
  @Override
  public String getRole() {
    return ROLE;
  }
  
  @Override
  protected void doBeforeConfigure(ClusterActionEvent event) throws IOException {
    Cluster cluster = event.getCluster();
    
    Set<Cluster.Instance> cldbInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRCLDBClusterActionHandler.ROLE));

    if (cldbInstances == null) {
        cldbInstances =
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        cldbInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

        /* Should set up firewall here ... but not sure how */
    Instance cldbnode = Iterables.getFirst(cldbInstances, null);
    event.getFirewallManager().addRules(
        Rule.create()
          .destination(cldbInstances)
          .port(MapRCluster.ZOOKEEPER_PORT)
    );
    
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    LOG.info("Completed configuration of {} role {}", clusterSpec.getClusterName(), getRole());
    InetAddress cldbPublicAddress = MapRCluster.getCLDBPublicAddress(cluster);
    InetAddress jobtrackerPublicAddress = MapRCluster.getJobTrackerPublicAddress(cluster);

    Properties config = createClientSideProperties(clusterSpec, cldbPublicAddress, jobtrackerPublicAddress);
    createClientSideHadoopSiteFile(clusterSpec, config);
    Properties combined = new Properties();
    combined.putAll(cluster.getConfiguration());
    combined.putAll(config);
    event.setCluster(new Cluster(cluster.getInstances(), combined));
  }

}
