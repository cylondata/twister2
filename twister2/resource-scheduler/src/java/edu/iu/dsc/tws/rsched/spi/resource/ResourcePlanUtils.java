//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.rsched.spi.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public final class ResourcePlanUtils {

  private ResourcePlanUtils() {
  }

  public static Map<String, List<ResourceContainer>> getContainersPerNode(
      List<ResourceContainer> containers) {
    Map<String, List<ResourceContainer>> containersPerNode = new HashMap<>();
    for (ResourceContainer c : containers) {
      String processName = (String) c.getProperty(SchedulerContext.WORKER_NAME);
      List<ResourceContainer> containerList;
      if (!containersPerNode.containsKey(processName)) {
        containerList = new ArrayList<>();
        containersPerNode.put(processName, containerList);
      } else {
        containerList = containersPerNode.get(processName);
      }
      containerList.add(c);
    }
    return containersPerNode;
  }
}
