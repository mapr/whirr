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
public class MapRTaskTrackerClusterActionHandler
                extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRTaskTrackerClusterActionHandler.class);

  public static final String TASK_TRACKER_ROLE = "mapr-tasktracker";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() {
    return TASK_TRACKER_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("TTHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, TASK_TRACKER_ROLE);

    LOG.info("TTHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("TTHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();


    // for each jt Instance, authorize firewall ingress
    Set<Cluster.Instance> ttInstances =
          cluster.getInstancesMatching(RolePredicates.role(TASK_TRACKER_ROLE));

    String ttPubIps = Joiner.on(',').join(
            MapRCommon.getPublicIps(ttInstances));
    LOG.info("TTHandler: Authorizing firewall for JobTrackers(s): {}", ttPubIps);

    if (! configuredFirewall) {
      configuredFirewall = true;

    }

    LOG.info("TTHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
