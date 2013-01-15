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
public class MapRHbaseRegionServerClusterActionHandler
                    extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRHbaseMasterClusterActionHandler.class);

  public static final String HbaseRegionServerRole = "mapr-hbase-regionserver";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() { return HbaseRegionServerRole; }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseRegionServerHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, HbaseRegionServerRole);

    LOG.info("HbaseRegionServerHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("HbaseRegionServerHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add CLDB firewall settings

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    ComputeServiceContext computeServiceContext =
            ComputeServiceContextBuilder.build(clusterSpec);

    if (! configuredFirewall) {
      configuredFirewall = true;

      // Add HBaseMaster web ui port to firewall.
      // Now add webserver to firewall settings
      Set<Cluster.Instance> hbaseRsInstances =
            cluster.getInstancesMatching(RolePredicates.role(HbaseRegionServerRole));

      String hbaseRsPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(hbaseRsInstances));

      LOG.info (
          "HbaseRegionServerHandler: Authorizing firewall for HbaseMasters(s): {}",
              hbaseRsPubServers);

      for (Cluster.Instance instance: hbaseRsInstances) {
        FirewallSettings.authorizeIngress(computeServiceContext, instance,
                clusterSpec, // instance.getPublicAddress().getHostAddress(),
                MapRCommon.HBASE_REGIONSERVER_WEB_PORT);

        break; // add only once since we dont have target address
      }
    }

    LOG.info("HbaseRegionServerHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
