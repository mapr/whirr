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

public class MapRJobTrackerClusterActionHandler extends MapRClusterActionHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MapRJobTrackerClusterActionHandler.class);
    
  public static final String ROLE = "mapr-jobtracker";
  
  @Override
  public String getRole() {
    return ROLE;
  }
  
  @Override
  protected void doBeforeConfigure(ClusterActionEvent event) throws IOException {
    Cluster cluster = event.getCluster();
    
    Set<Cluster.Instance> jtInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRJobTrackerClusterActionHandler.ROLE));

    if (jtInstances == null) {
        jtInstances =
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE));
    } else {
        jtInstances.addAll(
            cluster.getInstancesMatching(RolePredicates.role(
                  MapRMRMasterClusterActionHandler.ROLE)));
    }

        /* Not sure this is the right firewall setup here ... since 
         * external systems don't need access to JobTracker port
         */
    Instance jobtracker = Iterables.getFirst(jtInstances, null);
    event.getFirewallManager().addRules(
        Rule.create()
          .destination(RolePredicates.role(MapRJobTrackerClusterActionHandler.ROLE))
          .ports(MapRCluster.JOBTRACKER_PORT),
        Rule.create()
          .destination(RolePredicates.role(MapRMRMasterClusterActionHandler.ROLE))
          .ports(MapRCluster.JOBTRACKER_PORT)
    );
    
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    LOG.info("Completed configuration of {} role {}", clusterSpec.getClusterName(), getRole());

    InetAddress jobtrackerPublicAddress = MapRCluster.getJobTrackerPublicAddress(cluster);

  }
  
}
