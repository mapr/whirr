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
public class MapRWebServerClusterActionHandler
              extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRWebServerClusterActionHandler.class);

  public static final String WEB_SERVER_ROLE = "mapr-webserver";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return WEB_SERVER_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("WebServerHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, WEB_SERVER_ROLE);

    LOG.info("WebServerHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("WebServerHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    // Now add webserver to firewall settings
    if (! configuredFirewall) {
      configuredFirewall = true;

      Set<Cluster.Instance> wsInstances =
              cluster.getInstancesMatching(RolePredicates.role(WEB_SERVER_ROLE));

      String wsPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(wsInstances));

      LOG.info("WebServerHandler: Authorizing firewall for WebServer(s): {}",
              wsPubServers);

    }

    LOG.info("WebServerHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
