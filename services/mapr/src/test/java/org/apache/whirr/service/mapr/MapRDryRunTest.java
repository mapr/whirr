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

import com.google.common.base.Predicates;
// import static com.google.common.base.Predicates.and;
// import static com.google.common.base.Predicates.containsPattern;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

import com.google.common.base.Predicate;
import org.apache.whirr.service.BaseServiceDryRunTest;

public class MapRDryRunTest extends BaseServiceDryRunTest {

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of("mapr-zookeeper", "mapr-cldb", "mapr-jobtracker", 
        "mapr-fileserver", "mapr-tasktracker");
  }

  @Override
  protected Predicate<CharSequence> configurePredicate() {
    return Predicates.alwaysTrue();
//
//         These values are part of the mapr-metrics-ganglia.properties.vm
//         file, which won't be loaded given that we removed ganglia
//         from the InstanceRoles setting above.  Just bail out here
//    return and(
//        containsPattern("dfs.class=org.apache.mapr.metrics.ganglia.GangliaContext31"),
//        containsPattern("dfs.servers=10.1.1.1")
//    );
  }

  @Override
  protected Predicate<CharSequence> bootstrapPredicate() {
    return Predicates.alwaysTrue();
  }

}
