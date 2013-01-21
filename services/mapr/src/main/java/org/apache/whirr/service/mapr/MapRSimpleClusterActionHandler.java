package org.apache.whirr.service.mapr;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import org.apache.whirr.net.FastDnsResolver;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.ClusterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRSimpleClusterActionHandler extends ClusterActionHandlerSupport {
  private static final Logger LOG =
    LoggerFactory.getLogger(MapRSimpleClusterActionHandler.class);

  public static final String SIMPLE_ROLE = "mapr-simple";

  private InetAddress cldbPublicAddress = null;

  @Override
  public String getRole() {
    return SIMPLE_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("MapRSimpleHandler: beforeBootstrap(): Begin");
    

    MapRCommon.addCommonActions(this, event, SIMPLE_ROLE);

    LOG.info("MapRSimpleHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("MapRSimpleHandler: beforeConfig(): Begin");

    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    // pick the lowest private-ip & designate it as the cldb node.
    InetAddress cldbPrivateAddress = null;
    long cldbIP = Long.MAX_VALUE;
    Instance cldbInstance = null;

    Set<Instance> instances = cluster.getInstances();
    for (Instance inst: instances) {
      InetAddress addr = inst.getPrivateAddress();
      long ip = MapRCommon.ipToLong(addr.getAddress());
      if (ip < cldbIP) {
        cldbIP = ip;
        cldbPrivateAddress = addr;
        cldbPublicAddress = inst.getPublicAddress();
        cldbInstance = inst;
      }
    }

    LOG.info ("MapRSimpleHandler: CLDB PrivateIP={}, PublicIP={}",
      cldbPrivateAddress.getHostAddress(), cldbPublicAddress.getHostAddress());

    // Now do the firewall config from HadoopNameNodeClusterActionHandler
    LOG.info("MapRSimpleHandler: Authorizing firewall");


    MapRCommon.writeConfigureScript (
            clusterSpec,
            this,
            event,
            cldbPrivateAddress.getHostAddress(),
            cldbPrivateAddress.getHostAddress());

    LOG.info("MapRSimpleHandler: beforeConfig(): End");
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {

   LOG.info("MapRSimpleHandler: afterConfig(): Begin");

    // copied from HadoopNameNodeClusterActionHandler
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("MapRSimpleHandler: Completed configuration of {}",
            clusterSpec.getClusterName());

    FastDnsResolver fdr = new FastDnsResolver();
    LOG.info("MapRSimpleHandler: Web UI available at https://{}:{}",
        fdr.apply(cldbPublicAddress.getHostAddress()),
            MapRCommon.WEB_PORT);

    Properties config = MapRCommon.createClientSideProperties(
            clusterSpec, cldbPublicAddress, this.cldbPublicAddress);

    MapRCommon.createClientSideHadoopSiteFile(clusterSpec, config);
    MapRCommon.createProxyScript(clusterSpec, cluster, this.cldbPublicAddress);
    event.setCluster(new Cluster(cluster.getInstances(), config));

    LOG.info("MapRSimpleHandler: afterConfig(): End");
  }
}
