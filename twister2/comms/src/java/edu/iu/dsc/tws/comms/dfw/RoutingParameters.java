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
package edu.iu.dsc.tws.comms.dfw;

import java.util.HashSet;
import java.util.Set;

public class RoutingParameters {
  private Set<Integer> externalRoutes = new HashSet<>();
  private Set<Integer> internalRoutes = new HashSet<>();
  private int destinationId;

  public RoutingParameters(Set<Integer> externalRoutes,
                           Set<Integer> internalRoutes, int destinationId) {
    this.externalRoutes = externalRoutes;
    this.internalRoutes = internalRoutes;
    this.destinationId = destinationId;
  }

  public RoutingParameters() {
  }

  public Set<Integer> getExternalRoutes() {
    return externalRoutes;
  }

  public Set<Integer> getInternalRoutes() {
    return internalRoutes;
  }

  public int getDestinationId() {
    return destinationId;
  }

  public void addExternalRoutes(Set<Integer> extRoutes) {
    this.externalRoutes = extRoutes;
  }

  public void addInternalRoutes(Set<Integer> intlRoutes) {
    this.internalRoutes = intlRoutes;
  }

  public void addExternalRoute(int route) {
    this.externalRoutes.add(route);
  }

  public void addInteranlRoute(int route) {
    this.internalRoutes.add(route);
  }

  public void setDestinationId(int destId) {
    this.destinationId = destId;
  }

  @Override
  public String toString() {
    return "RoutingParameters{" + "externalRoutes=" + externalRoutes
        + ", internalRoutes=" + internalRoutes
        + ", destinationId=" + destinationId + '}';
  }
}
