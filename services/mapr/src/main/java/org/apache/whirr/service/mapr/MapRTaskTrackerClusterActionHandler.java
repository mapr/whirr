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

import org.apache.whirr.Cluster;
// import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MapRTaskTrackerClusterActionHandler extends MapRClusterActionHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MapRTaskTrackerClusterActionHandler.class);
    
  public static final String ROLE = "mapr-tasktracker";
  
  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void doBeforeConfigure(ClusterActionEvent event) 
  throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Beginning (no-op) configuration of {} role {}", clusterSpec.getClusterName(), getRole());
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    LOG.info("Completed (no-op) configuration of {} role {}", clusterSpec.getClusterName(), getRole());
  }
  
}
