//
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
import java.util.List;

/**
 * Holds information about the cluster resources including each container.
 */
public class ResourcePlan {
  // the cluster name
  private String cluster;

  // id of this task
  private int thisId;

  // list of resource containers
  private List<ResourceContainer> containers = new ArrayList<>();

  public ResourcePlan(String cluster, int id) {
    this.cluster = cluster;
    this.thisId = id;
  }

  public List<ResourceContainer> getContainers() {
    return containers;
  }

  public int noOfContainers() {
    return containers.size();
  }

  public void addContainer(ResourceContainer container) {
    this.containers.add(container);
  }

  public String getCluster() {
    return cluster;
  }

  public int getThisId() {
    return thisId;
  }
}
