/*
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
package org.apache.whirr.service.mapr.osgi;

import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.mapr.MapRZookeeperClusterActionHandler;
import org.apache.whirr.service.mapr.MapRCLDBClusterActionHandler;
import org.apache.whirr.service.mapr.MapRJobTrackerClusterActionHandler;
import org.apache.whirr.service.mapr.MapRFileServerClusterActionHandler;
import org.apache.whirr.service.mapr.MapRTaskTrackerClusterActionHandler;
import org.apache.whirr.service.mapr.MapRNFSClusterActionHandler;
import org.apache.whirr.service.mapr.MapRWebServerClusterActionHandler;
import org.apache.whirr.service.mapr.MapRMRMasterClusterActionHandler;
import org.jclouds.scriptbuilder.functionloader.osgi.BundleFunctionLoader;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.util.Properties;

public class Activator implements BundleActivator {

  private BundleFunctionLoader functionLoader;

  private final ClusterActionHandler fileServerClusterActionHandler = new MapRFileServerClusterActionHandler();
  private ServiceRegistration fileServerRegistration;

  private final ClusterActionHandler cldbClusterActionHandler = new MapRCLDBClusterActionHandler();
  private ServiceRegistration cldbRegistration;

  private final ClusterActionHandler zookeeperClusterActionHandler = new MapRZookeeperClusterActionHandler();
  private ServiceRegistration zookeeperRegistration;

  private final ClusterActionHandler jobTrackerClusterActionHandler = new MapRJobTrackerClusterActionHandler();
  private ServiceRegistration jobTrackerRegistration;

  private final ClusterActionHandler taskTrackerClusterActionHandler = new MapRTaskTrackerClusterActionHandler();
  private ServiceRegistration taskTrackerRegistration;

  private final ClusterActionHandler nfsClusterActionHandler = new MapRNFSClusterActionHandler();
  private ServiceRegistration nfsRegistration;

  private final ClusterActionHandler webServerClusterActionHandler = new MapRWebServerClusterActionHandler();
  private ServiceRegistration webServerRegistration;

  private final ClusterActionHandler mrMasterClusterActionHandler = new MapRMRMasterClusterActionHandler();
  private ServiceRegistration mrMasterRegistration;

  /**
   * Called when this bundle is started so the Framework can perform the
   * bundle-specific activities necessary to start this bundle. This method
   * can be used to register services or to allocate any resources that this
   * bundle needs.
   * <p/>
   * <p/>
   * This method must complete and return to its caller in a timely manner.
   *
   * @param context The execution context of the bundle being started.
   * @throws Exception If this method throws an exception, this
   *                   bundle is marked as stopped and the Framework will remove this
   *                   bundle's listeners, unregister all services registered by this
   *                   bundle, and release all services used by this bundle.
   */
  @Override
  public void start(BundleContext context) throws Exception {
    //Initialize OSGi based FunctionLoader
    functionLoader = new BundleFunctionLoader(context);
    functionLoader.start();

    Properties fileServerProps = new Properties();
    fileServerProps.put("name", fileServerClusterActionHandler.getRole());
    fileServerRegistration = context.registerService(ClusterActionHandler.class.getName(), fileServerClusterActionHandler, fileServerProps);

    Properties cldbProps = new Properties();
    cldbProps.put("name", cldbClusterActionHandler.getRole());
    cldbRegistration = context.registerService(ClusterActionHandler.class.getName(), cldbClusterActionHandler, cldbProps);

    Properties zookeeperProps = new Properties();
    zookeeperProps.put("name", zookeeperClusterActionHandler.getRole());
    zookeeperRegistration = context.registerService(ClusterActionHandler.class.getName(), zookeeperClusterActionHandler, zookeeperProps);

    Properties jobTrackerProps = new Properties();
    jobTrackerProps.put("name", jobTrackerClusterActionHandler.getRole());
    jobTrackerRegistration = context.registerService(ClusterActionHandler.class.getName(), jobTrackerClusterActionHandler, jobTrackerProps);

    Properties taskTrackerProps = new Properties();
    taskTrackerProps.put("name", taskTrackerClusterActionHandler.getRole());
    taskTrackerRegistration = context.registerService(ClusterActionHandler.class.getName(), taskTrackerClusterActionHandler, taskTrackerProps);

    Properties nfsProps = new Properties();
    nfsProps.put("name", nfsClusterActionHandler.getRole());
    nfsRegistration = context.registerService(ClusterActionHandler.class.getName(), nfsClusterActionHandler, nfsProps);

    Properties webServerProps = new Properties();
    webServerProps.put("name", webServerClusterActionHandler.getRole());
    webServerRegistration = context.registerService(ClusterActionHandler.class.getName(), webServerClusterActionHandler, webServerProps);

    Properties mrMasterProps = new Properties();
    mrMasterProps.put("name", mrMasterClusterActionHandler.getRole());
    mrMasterRegistration = context.registerService(ClusterActionHandler.class.getName(), mrMasterClusterActionHandler, mrMasterProps);
  }

  /**
   * Called when this bundle is stopped so the Framework can perform the
   * bundle-specific activities necessary to stop the bundle. In general, this
   * method should undo the work that the <code>BundleActivator.start</code>
   * method started. There should be no active threads that were started by
   * this bundle when this bundle returns. A stopped bundle must not call any
   * Framework objects.
   * <p/>
   * <p/>
   * This method must complete and return to its caller in a timely manner.
   *
   * @param context The execution context of the bundle being stopped.
   * @throws Exception If this method throws an exception, the
   *                   bundle is still marked as stopped, and the Framework will remove
   *                   the bundle's listeners, unregister all services registered by the
   *                   bundle, and release all services used by the bundle.
   */
  @Override
  public void stop(BundleContext context) throws Exception {
    if (fileServerRegistration != null) {
      fileServerRegistration.unregister();
    }
    if (cldbRegistration != null) {
      cldbRegistration.unregister();
    }
    if (zookeeperRegistration != null) {
      zookeeperRegistration.unregister();
    }
    if (jobTrackerRegistration != null) {
      jobTrackerRegistration.unregister();
    }
    if (taskTrackerRegistration != null) {
      taskTrackerRegistration.unregister();
    }

    if (nfsRegistration != null) {
      nfsRegistration.unregister();
    }

    if (webServerRegistration != null) {
      webServerRegistration.unregister();
    }

    if (mrMasterRegistration != null) {
      mrMasterRegistration.unregister();
    }

    if (functionLoader != null) {
      functionLoader.stop();
    }
  }
}
