/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.mapr;

// import static org.apache.whirr.service.mapr.MapRConfigurationBuilder.buildCommon;
// import static org.apache.whirr.service.mapr.MapRConfigurationBuilder.buildMapReduce;
// import static org.apache.whirr.service.mapr.MapRConfigurationBuilder.buildMapREnv;
// import static org.apache.whirr.service.mapr.MapRConfigurationBuilder.runConfigure;
import static org.jclouds.scriptbuilder.domain.Statements.call;
import static org.apache.whirr.RolePredicates.role;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.HttpURLConnection;
import java.net.URL;
// import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
// import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
// import org.apache.whirr.template.TemplateUtils;
import org.apache.whirr.net.FastDnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MapRClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(MapRClusterActionHandler.class);

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a MapR defaults
   * properties.
   */
  protected Configuration getConfiguration(
      ClusterSpec clusterSpec) throws IOException {
    return getConfiguration(clusterSpec, "whirr-mapr-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, "mapr", "install_mapr");
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, "mapr", "configure_mapr");
  }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {

    LOG.info ("MapRClusterActionHandler::beforeBootstrap() ... enter");

    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);



    addStatement(event, call("retry_helpers"));
//    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("configure_mapr_hostnames"));
    /* Core whirr supports both OpenJDK and Oracle JDK installations
     * Pick one and only one (only if the install_mapr.sh script 
     * doesn't install Java on its own) 
     */
    addStatement(event, call(getInstallFunction(conf, "java", "install_openjdk")));

        /* This is the location where we load up the 
         * repository details and invoke the installation script
         * NOTE: at this point, the "cluster" is not yet defined, so don't waste time
         * looking through event.getCluster() {it will be null} 
         */

    addStatement(event, call(getInstallFunction(conf)));

    LOG.info ("MapRClusterActionHandler::beforeBootstrap() ... exit");
  }
  
  protected Map<String, String> getDeviceMappings(ClusterActionEvent event) {
      Set<Instance> instances = event.getCluster().getInstancesMatching(RolePredicates.role(getRole()));
      Instance prototype = Iterables.getFirst(instances, null);
      if (prototype == null) {
          throw new IllegalStateException("No instances found in role " + getRole());
      }
      VolumeManager volumeManager = new VolumeManager();
      return volumeManager.getDeviceMappings(event.getClusterSpec(), prototype);
  }
    
  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    doBeforeConfigure(event);

    createMapRConfigScript(event, clusterSpec, cluster);

    addStatement(event, call("retry_helpers"));

    Integer nodeNum=1;
    if (cluster != null) {
        Integer numNodes = cluster.getInstances().size();
        event.getStatementBuilder().addExport("MapR_Nodes", numNodes.toString());
        for (Instance thisInst : cluster.getInstances()) {
            String id = thisInst.toString();
            String thisRole = Joiner.on(",").join(thisInst.getRoles());

            event.getStatementBuilder().addExport("MapR_Instance_"+nodeNum.toString()+"_Roles", thisRole);
            event.getStatementBuilder().addExportPerInstance(thisInst.getId(), "MapR_Instance_Num", nodeNum.toString());
            event.getStatementBuilder().addExportPerInstance(thisInst.getId(), "MapR_Instance_ID", id);
            nodeNum++;
        }
    } else {
        event.getStatementBuilder().addExport("MapR_Nodes", "0");
    }

    addStatement(event, call(
      getConfigureFunction(getConfiguration(clusterSpec)),
      Joiner.on(",").join(event.getInstanceTemplate().getRoles()),
      "-c", clusterSpec.getProvider())
    );
  }

    /* Sub-classes will override this rather than "beforeConfigure" */
  protected void doBeforeConfigure(ClusterActionEvent event) 
      throws IOException {} ;

  private void createMapRConfigScript(ClusterActionEvent event,
      ClusterSpec clusterSpec, Cluster cluster) throws IOException {

    LOG.info ("createMapRConfigScript ... enter");

    Configuration conf = getConfiguration(clusterSpec);

        // Identify CLDB and Zookeeper nodes
        // Those are nodes with the "unique" role name, or 
        // with a "master" in the role
    Set<Instance> mrMasterNodes = cluster
      .getInstancesMatching(role(MapRMRMasterClusterActionHandler.ROLE));

    Set<Cluster.Instance> cldbNodes =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRCLDBClusterActionHandler.ROLE));
    if (cldbNodes == null || cldbNodes.size() == 0) {
        cldbNodes = mrMasterNodes;
    } else {
        cldbNodes.addAll(mrMasterNodes) ;
    }

    Set<Cluster.Instance> zkNodes =
        cluster.getInstancesMatching(RolePredicates.role(
                  MapRZookeeperClusterActionHandler.ROLE));
    if (zkNodes == null || zkNodes.size() == 0) {
        zkNodes = mrMasterNodes;
    } else {
        zkNodes.addAll(mrMasterNodes) ;
    }

    String cldbServers=null, zkServers=null;
    if (cldbNodes != null && cldbNodes.size() > 0) {
      cldbServers = Joiner.on(',').join(getPrivateIps(cldbNodes));
    } else {
      LOG.info ("No CLDB nodes defined for cluser !!");
            /* we should error out here !!! */
    }

    if (zkNodes != null && zkNodes.size() > 0) {
      zkServers =Joiner.on(',').join(getPrivateIps(zkNodes));
    } else {
      LOG.info ("No Zookeeper nodes defined for cluser !!");
            /* we should error out here !!! */
    }

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

    addStatement (event, call(getConfigureFunction(conf), 
        uOpt, sOpt, cldbOpt, zkOpt, skipPartitioning)) ;

    String infoStr = String.format("%s %s %s %s %s",
          getConfigureFunction(conf), uOpt, sOpt, cldbOpt, zkOpt);

    LOG.info ("configure script generated: {}", infoStr);

    LOG.info ("createMapRConfigScript ... exit");
  }

  private String getMetricsTemplate(ClusterActionEvent event, ClusterSpec clusterSpec, Cluster cluster) {
    Configuration conf = clusterSpec.getConfiguration();
    if (conf.containsKey("mapr-metrics.template")) {
      return conf.getString("mapr-metrics.template");
    }
    
    Set<Instance> gmetadInstances = cluster.getInstancesMatching(RolePredicates.role("ganglia-metad"));
    if (!gmetadInstances.isEmpty()) {
      return "mapr-metrics-ganglia.properties.vm";
    }
    
    return "mapr-metrics-null.properties.vm";
  }

  public static Properties createClientSideProperties
    (ClusterSpec clusterSpec, InetAddress cldb, InetAddress jobtracker) 
      throws IOException {
    Properties config = new Properties();
    FastDnsResolver fdr = new FastDnsResolver();

    config.setProperty("hadoop.job.ugi", "root,root");
    config.setProperty("fs.default.name", String.format("maprfs://%s:%d/",
        fdr.apply(cldb.getHostAddress()), MapRCluster.CLDB_PORT));
    config.setProperty("mapred.job.tracker", String.format("%s:%d",
        fdr.apply(jobtracker.getHostAddress()), MapRCluster.JOBTRACKER_PORT));
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.net.SocksSocketFactory");

    if (clusterSpec.getProvider().endsWith("ec2")) {
      config.setProperty("fs.s3.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3.awsSecretAccessKey", clusterSpec.getCredential());
      config.setProperty("fs.s3n.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3n.awsSecretAccessKey", clusterSpec.getCredential());
    }

    return config;
  }

  public static void createClientSideHadoopSiteFile
      (ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "hadoop-site.xml");
    MapRConfigurationConverter.createClientSideHadoopSiteFile(hadoopSiteFile, config);
  }

  public static File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }
  
 public static void createProxyScript
        (ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    try {
      MapRProxy proxy = new MapRProxy(clusterSpec, cluster);
      InetAddress cldbnode = MapRCluster.getCLDBPublicAddress(cluster);
      String script = String.format("echo 'Running proxy to MapR cluster at %s. " +
          "Use Ctrl-c to quit.'\n", cldbnode.getHostName())
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      hadoopProxyFile.setExecutable(true);
      LOG.info("Wrote MapR proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing MapR proxy script {}", hadoopProxyFile, e);
    }
  }


    /* Utility functions since we have "lists" of CLDB and Zookeeper
     * nodes that will be used during configuration of the cluster
     */
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
              return instance.getPrivateAddress().getHostAddress();
            } catch (IOException e) {
              return null;
            }
          }
        });
      }

    /* Silly function to return class-C specification based on incoming IP */
    /* (borrowed from a private definition within the Fiewall class) */
    /*  DANGER : the logic of the Rule class doesn't allow this ... darn */
  public static String getOriginatingClassC() throws IOException {
    try {
      URL url = new URL("http://checkip.amazonaws.com/");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection(); 
      connection.connect();
      return IOUtils.toString(connection.getInputStream()).trim() + "/24";
    } catch (IOException e) {
      return "62.217.232.123/24";
    }
  }

}
