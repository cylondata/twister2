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
package edu.iu.dsc.tws.comms.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DirectRouter implements IRouter {
  private Set<Integer> sources;
  private int destinations;

  public DirectRouter(Set<Integer> srscs, int dest) {
    this.sources = srscs;
    this.destinations = dest;
  }

  @Override
  public Map<Integer, Routing> expectedRoutes() {
    Map<Integer, Routing> routingMap = new HashMap<>();

    List<Integer> down = new ArrayList<>();
    down.add(destinations);

    routingMap.put(0, new Routing(down));
    return routingMap;
  }

  @Override
  public boolean isSubRoute(int path, int source, int incomingSubEdge) {
    return false;
  }

  @Override
  public int subEdge(int path, int source, int incomingSubEdge) {
    return 0;
  }

  @Override
  public boolean isSubTask(int path, int source, int incomingEdge) {
    return false;
  }
}
