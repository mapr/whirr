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
public class MapRHbaseRegionServerClusterActionHandler
                    extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRHbaseMasterClusterActionHandler.class);

  public static final String HBASE_REGIONSERVER_ROLE = "mapr-hbase-regionserver";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() { return HBASE_REGIONSERVER_ROLE; }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseRegionServerHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, HBASE_REGIONSERVER_ROLE);

    LOG.info("HbaseRegionServerHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseRegionServerHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add CLDB firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    if (! configuredFirewall) {
      configuredFirewall = true;

      // Add HBaseMaster web ui port to firewall.
      // Now add webserver to firewall settings
      Set<Cluster.Instance> hbaseRsInstances =
            cluster.getInstancesMatching(RolePredicates.role(HBASE_REGIONSERVER_ROLE));

      String hbaseRsPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(hbaseRsInstances));

      LOG.info (
          "HbaseRegionServerHandler: Authorizing firewall for HbaseMasters(s): {}",
              hbaseRsPubServers);

    }

    LOG.info("HbaseRegionServerHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
