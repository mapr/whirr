package org.apache.whirr.service.mapr;

import org.apache.whirr.service.Service;

/**
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
@Deprecated
public class MapRZooKeeperService extends Service {
  @Override
  public String getName() {
    return MapRZooKeeperClusterActionHandler.ZookeeperRole;
  }
}
