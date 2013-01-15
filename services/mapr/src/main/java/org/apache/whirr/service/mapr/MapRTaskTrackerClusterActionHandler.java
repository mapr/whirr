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
public class MapRTaskTrackerClusterActionHandler
                extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRTaskTrackerClusterActionHandler.class);

  public static final String TaskTrackerRole = "mapr-tasktracker";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return TaskTrackerRole;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("TTHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, TaskTrackerRole);

    LOG.info("TTHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("TTHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);

    // for each jt Instance, authorize firewall ingress
    Set<Cluster.Instance> ttInstances =
          cluster.getInstancesMatching(RolePredicates.role(TaskTrackerRole));

    String ttPubIps = Joiner.on(',').join(
            MapRCommon.getPublicIps(ttInstances));
    LOG.info("TTHandler: Authorizing firewall for JobTrackers(s): {}", ttPubIps);

    if (! configuredFirewall) {
      configuredFirewall = true;

      for (Cluster.Instance instance: ttInstances) {
        FirewallSettings.authorizeIngress(computeServiceContext, instance,
            clusterSpec, // instance.getPublicAddress().getHostAddress(),
                MapRCommon.TASKTRACKER_WEB_UI_PORT);

        break; // add only once since we dont have target address
      }
    }

    LOG.info("TTHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
