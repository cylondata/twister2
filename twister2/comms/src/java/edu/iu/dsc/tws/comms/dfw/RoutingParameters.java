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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RoutingParameters {
  private List<Integer> externalRoutes = new ArrayList<>();
  private List<Integer> internalRoutes = new ArrayList<>();
  private int destinationId;

  public RoutingParameters(List<Integer> externalRoutes,
                           List<Integer> internalRoutes, int destinationId) {
    this.externalRoutes = externalRoutes;
    this.internalRoutes = internalRoutes;
    this.destinationId = destinationId;
  }

  public RoutingParameters() {
  }

  public List<Integer> getExternalRoutes() {
    return externalRoutes;
  }

  public List<Integer> getInternalRoutes() {
    return internalRoutes;
  }

  public int getDestinationId() {
    return destinationId;
  }

  public void addExternalRoutes(Collection<Integer> extRoutes) {
    this.externalRoutes.addAll(extRoutes);
  }

  public void addInternalRoutes(Collection<Integer> intlRoutes) {
    this.internalRoutes.addAll(intlRoutes);
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
