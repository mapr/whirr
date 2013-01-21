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
public class MapRHbaseMasterClusterActionHandler
                extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRHbaseMasterClusterActionHandler.class);

  public static final String HBASE_MASTER_ROLE = "mapr-hbase-master";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() { return HBASE_MASTER_ROLE; }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseMasterHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, HBASE_MASTER_ROLE);

    LOG.info("HbaseMasterHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseMasterHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add CLDB firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    if (! configuredFirewall) {
      configuredFirewall = true;

      // Add HBaseMaster web ui port to firewall.
      // Now add webserver to firewall settings
      Set<Cluster.Instance> hbaseMasterInstances =
            cluster.getInstancesMatching(RolePredicates.role(HBASE_MASTER_ROLE));

      String hbaseMasterPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(hbaseMasterInstances));

      LOG.info("HbaseMasterHandler: Authorizing firewall for HbaseMasters(s): {}",
              hbaseMasterPubServers);

    }

    LOG.info("HbaseMasterHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
