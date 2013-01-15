package org.apache.whirr.service.mapr;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.whirr.net.FastDnsResolver;
import org.apache.whirr.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */

// Utils class to do common stuff
public class MapRCommon {
  private static final Logger LOG = LoggerFactory.getLogger(MapRCommon.class);

  public static final int NAMENODE_WEB_UI_PORT   = 7221;
  public static final int JOBTRACKER_WEB_UI_PORT = 50030;
  public static final int HBASE_MASTER_WEB_PORT  = 60010;

  public static final int WEB_PORT        = 8443;
  public static final int NAMENODE_PORT   = 7222;
  public static final int JOBTRACKER_PORT = 9001;
  public static final int FILESERVER_PORT = 5660;

  public static final int TASKTRACKER_WEB_UI_PORT     = 50060;
  public static final int HBASE_REGIONSERVER_WEB_PORT = 60030;


  public static Properties
      createClientSideProperties(ClusterSpec clusterSpec,
                                 InetAddress namenode,
                                 InetAddress jobtracker)
          throws IOException {
    Properties config = new Properties();
    FastDnsResolver fdr = new FastDnsResolver();

    config.setProperty("hadoop.job.ugi", "root,root");
    config.setProperty("fs.default.name", String.format("maprfs://%s:%d/",
          fdr.apply(namenode.getHostAddress()), NAMENODE_PORT));
    config.setProperty("mapred.job.tracker", String.format("%s:%d",
            fdr.apply(jobtracker.getHostAddress()), JOBTRACKER_PORT));
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default",
                          "org.apache.hadoop.net.SocksSocketFactory");

    if ("ec2".equals(clusterSpec.getProvider())) {
      config.setProperty("fs.s3.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3.awsSecretAccessKey", clusterSpec.getCredential());
      config.setProperty("fs.s3n.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3n.awsSecretAccessKey", clusterSpec.getCredential());
    }

    return config;
  }

  public static void createClientSideHadoopSiteFile(ClusterSpec clusterSpec,
                                                    Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "hadoop-site.xml");

    try {
      Files.write(generateHadoopConfigurationFile(config), hadoopSiteFile,
              Charsets.UTF_8);
      LOG.info("Wrote Hadoop site file {}", hadoopSiteFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop site file {}", hadoopSiteFile, e);
    }
  }

  private static File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }

  private static CharSequence generateHadoopConfigurationFile(Properties config) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\"?>\n");
    sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
    sb.append("<configuration>\n");
    for (Map.Entry<Object, Object> entry : config.entrySet()) {
      sb.append("  <property>\n");
      sb.append("    <name>").append(entry.getKey()).append("</name>\n");
      sb.append("    <value>").append(entry.getValue()).append("</value>\n");
      sb.append("  </property>\n");
    }
    sb.append("</configuration>\n");
    return sb;
  }

  public static void createProxyScript(ClusterSpec clusterSpec,
                                       Cluster cluster,
                                       InetAddress cldbPublicAddress) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    FastDnsResolver fdr = new FastDnsResolver();
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      String script = String.format("echo 'Running proxy to Hadoop cluster at %s. " +
          "Use Ctrl-c to quit.'\n",
          fdr.apply(cldbPublicAddress.getHostAddress()))
          + Joiner.on(" ").join(proxy.getProxyCommand(cldbPublicAddress));
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      LOG.info("Wrote Hadoop proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop proxy script {}", hadoopProxyFile, e);
    }
  }

  public static void addCommonActions(ClusterActionHandlerSupport handler,
                                      ClusterActionEvent event,
                                      String role)
          throws IOException, InterruptedException {
    LOG.info("addCommonActions(): Begin");

    ClusterSpec clusterSpec = event.getClusterSpec();
    handler.addRunUrl(event, "sun/java/install");

    String maprInstallRunUrl = clusterSpec.getConfiguration().getString(
        "whirr.mapr-install-runurl", "mapr/install");

    String repoServer = clusterSpec.getConfiguration().getString (
        "whirr.mapr-repository-server", null);
    String repoUserName = clusterSpec.getConfiguration().getString (
        "whirr.mapr-repository-username", null);
    String repoUserPwd = clusterSpec.getConfiguration().getString (
        "whirr.mapr-repository-password", null);

    StringBuilder sb = new StringBuilder();

    if (repoServer != null) {
      sb.append("-s ").append(repoServer).append(' ');
    }

    if (repoUserName != null) {
      sb.append("-u ").append(repoUserName).append(' ');
    }

    if (repoUserPwd != null) {
      sb.append("-p ").append(repoUserPwd).append(' ');
    }

    String repoInfo = sb.toString();

    /**
    String skipCore = clusterSpec.getConfiguration().getString(
            "whirr.mapr-skipcore", null);

    if (skipCore == null || skipCore.length() == 0) {
      // mapr-core is need by all packages. So add it in unless we have been
      // told to skip it.
      handler.addRunUrl(event, maprInstallRunUrl,
            "-c", clusterSpec.getProvider(),
            "-r", "mapr-core",
            creds);  // all packges need mapr-core
    }
    */

    handler.addRunUrl(event, maprInstallRunUrl,
            "-c", clusterSpec.getProvider(),
            "-r", role,
            repoInfo);

    LOG.info("addCommonActions(): End");
  }

  public static List<String> getPrivateIps(Set<Cluster.Instance> instances) 
    throws IOException {
    return Lists.transform(Lists.newArrayList(instances),
            new Function<Cluster.Instance, String>() {
              @Override
              public String apply(Cluster.Instance instance) {
                try {
                  return instance.getPrivateAddress().getHostAddress();
                } catch (IOException e) {
                  return null;
                }
              }
            });
  }

  public static List<String> getPublicIps(Set<Cluster.Instance> instances) 
    throws IOException {
    return Lists.transform(Lists.newArrayList(instances),
            new Function<Cluster.Instance, String>() {
              @Override
              public String apply(Cluster.Instance instance) {
                try {
                return instance.getPublicAddress().getHostAddress();
                } catch (IOException e) {
                  return null;
                }
              }
            });
  }

  public static long ipToLong (byte[] address) {
    long ip = 0;
    for (int i=0; i < address.length; ++i) {
      ip <<= 8;
      ip |= (address[i] & 0xFF);
    }

    return ip;
  }

  public static void doConfigure (ClusterActionHandlerSupport handler,
                                  ClusterActionEvent event)
          throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    // Get All instances of type cldb & add it all to the url
    Set<Cluster.Instance> cldbInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRCldbClusterActionHandler.CLDB_ROLE));

    Set<Cluster.Instance> zkInstances =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRZooKeeperClusterActionHandler.ZOOKEEPER_ROLE));

    String cldbServers=null, zkServers=null;
    if (cldbInstances != null && cldbInstances.size() > 0) {
      cldbServers = Joiner.on(',').join(
              MapRCommon.getPrivateIps(cldbInstances));
    } else {
      LOG.info ("did not find any CLDB instances !!");
    }

    if (zkInstances != null && zkInstances.size() > 0) {
      zkServers =Joiner.on(',').join(
              MapRCommon.getPrivateIps(zkInstances));
    } else {
      LOG.info ("did not find any Zk instances !!");
    }

    writeConfigureScript(clusterSpec, handler, event, cldbServers, zkServers);
  }

  public static void writeConfigureScript (ClusterSpec clusterSpec,
                                           ClusterActionHandlerSupport handler,
                                           ClusterActionEvent event,
                                           String cldbServers,
                                           String zkServers)
        throws IOException {
    String hadoopConfigureRunUrl = clusterSpec.getConfiguration().getString(
        "whirr.mapr-configure-runurl", "mapr/configure");

    String unmountPaths = clusterSpec.getConfiguration().getString (
        "whirr.mapr-unmount", null);

    String skipDevices = clusterSpec.getConfiguration().getString (
        "whirr.mapr-skip-devices", null);

    String uOpt = unmountPaths == null ? "": "-u " + unmountPaths;
    String sOpt = skipDevices  == null ? "": "-s " + skipDevices;

    String cldbOpt = cldbServers == null ? "": "-n " + cldbServers;
    String zkOpt   = zkServers   == null ? "": "-z " + zkServers;

    boolean skip = clusterSpec.getConfiguration().getBoolean  (
        "whirr.mapr-skip-partitioning", false);
    String skipPartitioning = skip ? "-d": "";

    handler.addRunUrl(event, hadoopConfigureRunUrl,
        uOpt,
        sOpt,
        cldbOpt,
        zkOpt,
        skipPartitioning,
        "-c", clusterSpec.getProvider());

    String infoStr = String.format("wrote configure script: %s %s %s %s %s",
          hadoopConfigureRunUrl, uOpt, sOpt, cldbOpt, zkOpt);

    LOG.info ("wrote configure script: {}", infoStr);
  }

  public static boolean hasInstanceOf (ClusterActionEvent event, String role) {
    Cluster cluster = event.getCluster();

    Set<Cluster.Instance> instances = cluster.getInstancesMatching (
        RolePredicates.role(role));

    boolean hasInst = instances != null && instances.size() > 0;
    return hasInst;
  }

}
