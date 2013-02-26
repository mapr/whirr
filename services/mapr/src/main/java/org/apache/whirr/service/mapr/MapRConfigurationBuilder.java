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

import static org.apache.whirr.RolePredicates.role;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Processor;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRConfigurationBuilder {
 
  private static final Logger LOG = LoggerFactory.getLogger(MapRConfigurationBuilder.class);
 
  private static final String WHIRR_MAPR_DEFAULT_PROPERTIES =
    "whirr-mapr-default.properties";

  private static Configuration build(ClusterSpec clusterSpec, Cluster cluster,
      Configuration defaults, String prefix)
      throws ConfigurationException {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setDelimiterParsingDisabled(true);
    Configuration sub = clusterSpec.getConfigurationForKeysWithPrefix(prefix);
    config.addConfiguration(sub.subset(prefix)); // remove prefix
    config.addConfiguration(defaults.subset(prefix));
    return config;
  }

  public static Statement buildCommon(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildCommonConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(MapRConfigurationBuilder.class.getResource("/" + WHIRR_MAPR_DEFAULT_PROPERTIES)));
    return MapRConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }
  
  public static Statement buildMapReduce(String path, ClusterSpec clusterSpec,
        Cluster cluster, Set<String> dataDirectories) throws ConfigurationException, IOException {
    Configuration config = buildMapReduceConfiguration(clusterSpec, cluster,
      new PropertiesConfiguration(MapRConfigurationBuilder.class.getResource("/" + WHIRR_MAPR_DEFAULT_PROPERTIES)), dataDirectories);
    return MapRConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
}

  public static Statement buildMapREnv(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildMapREnvConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(MapRConfigurationBuilder.class.getResource("/" + WHIRR_MAPR_DEFAULT_PROPERTIES)));
    return MapRConfigurationConverter.asCreateEnvironmentVariablesFileStatement(path, config);
  }
  
  @VisibleForTesting
  static Configuration buildCommonConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults,
        "mapr-core");

        /* Pull the list of ZK and CLDB nodes from the Cluster definitions ...
           that's all nodes that are explicitly "master" nodes or
           ones that have cldb and zookeeper listed separately.
        */
    
    Set<Instance> mrMasterNodes = cluster
      .getInstancesMatching(role(MapRMRMasterClusterActionHandler.ROLE));

    Set<Instance> cldbNodes = cluster
      .getInstancesMatching(role(MapRCLDBClusterActionHandler.ROLE));

    if (cldbNodes == null  ||  cldbNodes.size() == 0) {
        cldbNodes = mrMasterNodes ;
    } else if (mrMasterNodes != null && mrMasterNodes.size() > 0) {
        cldbNodes.addAll(mrMasterNodes) ;
    }


    Set<Instance> zkNodes = cluster
      .getInstancesMatching(role(MapRZookeeperClusterActionHandler.ROLE));

    if (zkNodes == null  ||  zkNodes.size() == 0) {
        zkNodes = mrMasterNodes ;
    } else if (mrMasterNodes != null && mrMasterNodes.size() > 0) {
        zkNodes.addAll(mrMasterNodes) ;
    }

/*
    Instance namenode = cluster
        .getInstanceMatching(role(MapRCLDBClusterActionHandler.ROLE));
    LOG.debug("hadoop building common configuration, with hostname "+namenode.getPublicHostName());
    config.setProperty("fs.default.name", String.format("hdfs://%s:8020/",
        namenode.getPublicHostName()));
*/

    return config;
  }
  
  @VisibleForTesting
  static Configuration buildMapReduceConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults, Set<String> dataDirectories) throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults,
        "mapr-mapreduce");

    setIfAbsent(config, "mapred.local.dir",
        appendToDataDirectories(dataDirectories, "/mapred/local"));

    Set<Instance> taskTrackers = cluster
      .getInstancesMatching(role(MapRTaskTrackerClusterActionHandler.ROLE));

    if (!taskTrackers.isEmpty()) {

      Hardware hardware = Iterables.getFirst(taskTrackers, null)
        .getNodeMetadata().getHardware();

      /* null when using the BYON jclouds compute provider */
      if (hardware != null) {

        int coresPerNode = 0;
        for (Processor processor : hardware.getProcessors()) {
          coresPerNode += processor.getCores();
        }
        int mapTasksPerNode = (int) Math.ceil(coresPerNode * 1.0);
        int reduceTasksPerNode = (int) Math.ceil(coresPerNode * 0.75);

        setIfAbsent(config, "mapred.tasktracker.map.tasks.maximum", mapTasksPerNode + "");
        setIfAbsent(config, "mapred.tasktracker.reduce.tasks.maximum", reduceTasksPerNode + "");

        int clusterReduceSlots = taskTrackers.size() * reduceTasksPerNode;
        setIfAbsent(config, "mapred.reduce.tasks", clusterReduceSlots + "");
      }
    }

    Set<Instance> jobtracker = cluster
        .getInstancesMatching(role(MapRJobTrackerClusterActionHandler.ROLE));
    if (!jobtracker.isEmpty()) {
      config.setProperty("mapred.job.tracker", String.format("%s:8021",
          Iterables.getOnlyElement(jobtracker).getPublicHostName()));
    }

    return config;
  }


  @VisibleForTesting
  static Configuration buildMapREnvConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException {
        return build(clusterSpec, cluster, defaults, "mapr-env");
    }

  
  private static void setIfAbsent(Configuration config, String property, String value) {
    if (!config.containsKey(property)) {
      config.setProperty(property, value);
    }
  }
  
  private static String appendToDataDirectories(Set<String> dataDirectories, final String suffix) {
    return Joiner.on(',').join(Lists.transform(Lists.newArrayList(dataDirectories),
      new Function<String, String>() {
        @Override public String apply(String input) {
          return input + suffix;
        }
      }
    ));
  }

}
