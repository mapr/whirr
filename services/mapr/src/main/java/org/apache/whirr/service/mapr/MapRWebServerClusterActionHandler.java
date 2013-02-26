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
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRWebServerClusterActionHandler extends MapRClusterActionHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MapRWebServerClusterActionHandler.class);

  public static final String ROLE = "mapr-webserver";
  
  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
        /* Poor man's firewall setting ... let everyone access WEB_PORT from outside cluster 
         * change to ' .source("0.0.0.0/0")'  if you need absolute openness.
         *
         * NOTE: For this class only, the Firewall settings are done at the end
         * of the configuration phase, since it's not likely that the web port is needed
         * any earlier than that.
         */
    event.getFirewallManager().addRules(
        Rule.create()
          .destination(RolePredicates.role(ROLE))
          .port(MapRCluster.WEB_PORT)
    );
    
    LOG.info("Completed configuration of {} role {}", clusterSpec.getClusterName(), getRole());
  }
  
}

