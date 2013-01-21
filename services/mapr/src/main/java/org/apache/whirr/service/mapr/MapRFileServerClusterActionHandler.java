package org.apache.whirr.service.mapr;

import com.google.common.base.Joiner;
import org.apache.whirr.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
public class MapRFileServerClusterActionHandler
              extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRFileServerClusterActionHandler.class);

  public static final String FILE_SERVER_ROLE = "mapr-fileserver";

  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return FILE_SERVER_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {

    MapRCommon.addCommonActions(this, event, FILE_SERVER_ROLE);
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {

    super.beforeConfigure(event, false); // dont add CLDB firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    Set<Cluster.Instance> fsInstances =
          cluster.getInstancesMatching(RolePredicates.role(FILE_SERVER_ROLE));

    String fsPubServers = Joiner.on(',').join(
            MapRCommon.getPublicIps(fsInstances));

    if (! configuredFirewall) {
      configuredFirewall = true;
      LOG.info("FSHandler: Authorizing firewall for fs port(s): {}",
              fsPubServers);
    }

  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
