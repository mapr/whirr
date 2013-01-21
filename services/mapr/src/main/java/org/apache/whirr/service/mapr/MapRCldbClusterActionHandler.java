package org.apache.whirr.service.mapr;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterActionEvent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
public class MapRCldbClusterActionHandler extends ClusterActionHandlerSupport {
  private static final Logger LOG =
    LoggerFactory.getLogger(MapRCldbClusterActionHandler.class);

  public static final String CLDB_ROLE = "mapr-cldb";
  private boolean configuredFirewall = false;

  @Override
  public String getRole() { return CLDB_ROLE; }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("CLDBHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, CLDB_ROLE);

    LOG.info("CLDBHander: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    beforeConfigure(event, true);
  }

  protected void beforeConfigure(ClusterActionEvent event,
                                 boolean addCldbPortsToFirewall)
        throws IOException, InterruptedException {
    LOG.info("CLDBHandler: beforeConfig(): Begin: {}", this);

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();


    // add configure URL
    MapRCommon.doConfigure(this, event);

    // Now do the firewall config from HadoopNameNodeClusterActionHandler
    // for each cldb Instance, authorize firewall ingress

    if (addCldbPortsToFirewall && !configuredFirewall) {
      configuredFirewall = true;

      Set<Cluster.Instance> cldbInstances =
              cluster.getInstancesMatching(RolePredicates.role(CLDB_ROLE));

      String cldbPubServers = Joiner.on(',').join(
              MapRCommon.getPublicIps(cldbInstances));

      LOG.info("CLDBHander: Authorizing firewall for CLDB(s): {}",
              cldbPubServers);

    }

    LOG.info("CLDBHander: beforeConfig(): End");
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
   LOG.info("CLDBHander: afterConfig(): Begin");

    // copied from HadoopNameNodeClusterActionHandler
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("CLDBHander: Completed configuration of {}",
            clusterSpec.getClusterName());

    // pick the lowest private-ip & designate it as the cldb node.
    InetAddress cldbPublicAddress = null;
    long cldbIP = Long.MAX_VALUE;

    Set<Cluster.Instance> instances = cluster.getInstancesMatching (
        RolePredicates.role(CLDB_ROLE));
    for (Cluster.Instance inst: instances) {
      InetAddress pAddr = inst.getPrivateAddress();
      long ip = MapRCommon.ipToLong(pAddr.getAddress());
      if (ip < cldbIP) {
        cldbIP = ip;
        cldbPublicAddress = inst.getPublicAddress();
      }
    }

    // similarly pick a job-tracker ip.
    InetAddress jtPubAddress = null;
    instances = cluster.getInstancesMatching (
            RolePredicates.role(MapRJobTrackerClusterActionHandler.JOB_TRACKER_ROLE));

    long jtIP = Long.MAX_VALUE;
    for (Cluster.Instance inst: instances) {
      InetAddress pAddr = inst.getPrivateAddress();
      long ip = MapRCommon.ipToLong(pAddr.getAddress());
      if (ip < jtIP) {
        jtIP = ip;
        jtPubAddress = inst.getPublicAddress();
      }
    }

    if (jtPubAddress == null) jtPubAddress = cldbPublicAddress;

    LOG.info ("CLDBHander: Setting CLDB host = {}, JT host = {}",
          cldbPublicAddress, jtPubAddress);

    // create a client-side proxy..
    Properties config = MapRCommon.createClientSideProperties(
            clusterSpec, cldbPublicAddress, jtPubAddress);

    MapRCommon.createClientSideHadoopSiteFile(clusterSpec, config);
    MapRCommon.createProxyScript(clusterSpec, cluster, cldbPublicAddress);
      event.setCluster(new Cluster(cluster.getInstances(), config));

    LOG.info("CLDBHander: afterConfig(): End");
  }
}
