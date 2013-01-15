package org.apache.whirr.service.mapr;

import org.apache.whirr.service.Service;

/**
 * User: sathya
 */

@Deprecated
public class MapRSimpleService extends Service {
  @Override
  public String getName() {
    return MapRSimpleClusterActionHandler.SimpleRole;
  }
}
