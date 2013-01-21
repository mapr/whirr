package org.apache.whirr.service.mapr;

import org.apache.whirr.service.ClusterActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
public class MapRZooKeeperClusterActionHandler
                extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(MapRZooKeeperClusterActionHandler.class);

  public static final String ZOOKEEPER_ROLE = "mapr-zookeeper";

  @Override
  public String getRole() {
    return ZOOKEEPER_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("ZooKeeperHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, ZOOKEEPER_ROLE);

    LOG.info("ZooKeeperHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("ZooKeeperHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    LOG.info("ZooKeeperHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
