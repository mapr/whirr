package org.apache.whirr.service.mapr;

import org.apache.whirr.service.ClusterActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
public class MapRNfsClusterActionHandler extends MapRCldbClusterActionHandler {
  private static final Logger LOG =
    LoggerFactory.getLogger(MapRNfsClusterActionHandler.class);

  public static final String NfsRole = "mapr-nfs";

  @Override
  public String getRole() {
    return NfsRole;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("NFSHandler: beforeBootstrap(): Begin");

    MapRCommon.addCommonActions(this, event, NfsRole);

    LOG.info("NFSHandler: beforeBootstrap(): End");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
          throws IOException, InterruptedException {
    LOG.info("NFSHandler: beforeConfigure(): Begin");

    super.beforeConfigure(event, false); // dont add firewall settings

    LOG.info("NFSHandler: beforeConfigure(): End");
  }

  @Override
  protected void afterConfigure (ClusterActionEvent event)
          throws IOException, InterruptedException {
    // empty don't set up the client-side proxy stuff
  }
}
