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
public class MapRFileServerClusterActionHandler
              extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRFileServerClusterActionHandler.class);

  public static final String FileServerRole = "mapr-fileserver";

  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return FileServerRole;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("FSHandler: beforeBootstrap(): Begin: {}", this);

    MapRCommon.addCommonActions(this, event, FileServerRole);

    LOG.info("FSHandler: beforeBootstrap(): End {}", this);
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("FSHandler: beforeConfigure(): Begin: {}", this);

    super.beforeConfigure(event, false); // dont add CLDB firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    ComputeServiceContext computeServiceContext =
        ComputeServiceContextBuilder.build(clusterSpec);

    Set<Cluster.Instance> fsInstances =
          cluster.getInstancesMatching(RolePredicates.role(FileServerRole));

    String fsPubServers = Joiner.on(',').join(
            MapRCommon.getPublicIps(fsInstances));

    if (! configuredFirewall) {
      configuredFirewall = true;
      LOG.info("FSHandler: Authorizing firewall for fs port(s): {}: {}",
              fsPubServers, this);

      for (Cluster.Instance instance: fsInstances) {
        FirewallSettings.authorizeIngress(computeServiceContext, instance,
                clusterSpec,
                // instance.getPublicAddress().getHostAddress(),
                MapRCommon.FILESERVER_PORT);
        break; // add only once since we dont use target address
      }
    }

    LOG.info("FSHandler: beforeConfigure(): End {}", this);
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
