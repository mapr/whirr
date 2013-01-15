package org.apache.whirr.service.mapr;

import com.google.common.base.Joiner;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.RolePredicates;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
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

  public static final String WebServerRole = "mapr-webserver";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return WebServerRole;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("WebServerHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, WebServerRole);

    LOG.info("WebServerHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("WebServerHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    ComputeServiceContext computeServiceContext =
            ComputeServiceContextBuilder.build(clusterSpec);

    // Now add webserver to firewall settings
    if (! configuredFirewall) {
      configuredFirewall = true;

      Set<Cluster.Instance> wsInstances =
              cluster.getInstancesMatching(RolePredicates.role(WebServerRole));

      String wsPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(wsInstances));

      LOG.info("WebServerHandler: Authorizing firewall for WebServer(s): {}",
              wsPubServers);

      for (Cluster.Instance instance: wsInstances) {
        FirewallSettings.authorizeIngress (computeServiceContext, instance,
            clusterSpec, // instance.getPublicAddress().getHostAddress(),
                MapRCommon.WEB_PORT);

        break; // add only once since we dont have target address
      }
    }

    LOG.info("WebServerHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
