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

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Volume;

public class VolumeManager {
  
  public static final String MOUNT_PREFIX = "/data";
  
  public Map<String, String> getDeviceMappings(ClusterSpec clusterSpec, Instance instance) {
    Map<String, String> mappings = Maps.newLinkedHashMap();
    int number = 0;
    Hardware hardware = instance.getNodeMetadata().getHardware();

    /* null when using the BYON jclouds compute provider */
    if (hardware != null) {
        List<? extends Volume> volumes =
            instance.getNodeMetadata().getHardware().getVolumes();
        for (Volume volume : volumes) {
            if (volume.isBootDevice()) {
                continue;
            }
            
            mappings.put(MOUNT_PREFIX + number++, volume.getDevice());
        }
    }
    return mappings;
  }
  
  public static String asString(Map<String, String> deviceMappings) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> mapping : deviceMappings.entrySet()) {
      if (sb.length() > 0) {
        sb.append(";");
      }
      sb.append(mapping.getKey()).append(",").append(mapping.getValue());
    }
    return sb.toString();
  }
}
