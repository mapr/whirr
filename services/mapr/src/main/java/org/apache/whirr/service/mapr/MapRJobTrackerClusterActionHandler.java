package org.apache.whirr.service.mapr;

import com.google.common.base.Joiner;
import org.apache.whirr.service.*;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
public class MapRJobTrackerClusterActionHandler
        extends ClusterActionHandlerSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(MapRJobTrackerClusterActionHandler.class);

  public static final String JobTrackerRole = "mapr-jobtracker";

  private boolean configuredFirewall = false;

  @Override
  public String getRole() { return JobTrackerRole; }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("JTHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, JobTrackerRole);

    LOG.info("JTHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("JTHandler: beforeConfig(): Begin");

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);

    MapRCommon.doConfigure(this, event);

    // for each jt Instance, authorize firewall ingress
    Set<Cluster.Instance> jtInstances =
          cluster.getInstancesMatching(RolePredicates.role(JobTrackerRole));

    String jtPubIps = Joiner.on(',').join(
            MapRCommon.getPublicIps(jtInstances));
    LOG.info("JTHandler: Authorizing firewall for JobTrackers(s): {}", jtPubIps);

    if (! configuredFirewall) {
      configuredFirewall = true;

      for (Cluster.Instance instance: jtInstances) {
        FirewallSettings.authorizeIngress(computeServiceContext, instance,
            clusterSpec, // instance.getPublicAddress().getHostAddress(),
                MapRCommon.JOBTRACKER_PORT);

        FirewallSettings.authorizeIngress(computeServiceContext, instance,
            clusterSpec, // instance.getPublicAddress().getHostAddress(),
                MapRCommon.JOBTRACKER_WEB_UI_PORT);

        break; // add only once since we dont have target address
      }
    }

    LOG.info("JTHandler: beforeConfig(): End");
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
   LOG.info("JTHandler: afterConfig(): Begin");

    // copied from HadoopNameNodeClusterActionHandler
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("JTHandler: Completed configuration of {}",
            clusterSpec.getClusterName());

    // similarly pick a job-tracker ip.
    InetAddress jtPubAddress = null;
    Set<Cluster.Instance> instances = cluster.getInstancesMatching (
        RolePredicates.role(MapRJobTrackerClusterActionHandler.JobTrackerRole));

    long jtIP = Long.MAX_VALUE;
    for (Cluster.Instance inst: instances) {
      InetAddress pAddr = inst.getPrivateAddress();
      long ip = MapRCommon.ipToLong(pAddr.getAddress());
      if (ip < jtIP) {
        jtIP = ip;
        jtPubAddress = inst.getPublicAddress();
      }
    }

    // pick the lowest private-ip & designate it as the cldb node.
    InetAddress cldbPublicAddress = null;
    long cldbIP = Long.MAX_VALUE;

    instances = cluster.getInstancesMatching (
        RolePredicates.role(MapRCldbClusterActionHandler.CldbRole));
    for (Cluster.Instance inst: instances) {
      InetAddress pAddr = inst.getPrivateAddress();
      long ip = MapRCommon.ipToLong(pAddr.getAddress());
      if (ip < cldbIP) {
        cldbIP = ip;
        cldbPublicAddress = inst.getPublicAddress();
      }
    }

    if (cldbPublicAddress == null) cldbPublicAddress = jtPubAddress;

    LOG.info ("JTHandler: Setting CLDB host = {}, JT host = {}",
          cldbPublicAddress, jtPubAddress);

    // create a client-side proxy..
    Properties config = MapRCommon.createClientSideProperties(
            clusterSpec, cldbPublicAddress, jtPubAddress);

    MapRCommon.createClientSideHadoopSiteFile(clusterSpec, config);
    MapRCommon.createProxyScript(clusterSpec, cluster, cldbPublicAddress);
      event.setCluster(new Cluster(cluster.getInstances(), config));

    LOG.info("JTHandler: afterConfig(): End");
  }
}
