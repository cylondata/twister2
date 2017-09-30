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
import java.util.Random;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class LoadBalanceRouter implements IRouter {
  private Config config;
  private TaskPlan taskPlan;
  private Set<Integer> sources;
  private int stream;
  private int task;
  private List<Integer> destList;
  private Random random;

  @Override
  public void init(Config cfg, int thisTask, TaskPlan plan,
                   Set<Integer> srscs, Set<Integer> dests,
                   int strm, int distinctRoutes) {
    this.config = cfg;
    this.taskPlan = plan;
    this.task = thisTask;
    this.sources = srscs;
    this.stream = strm;
    random = new Random(System.nanoTime());

    destList = new ArrayList<>(dests);
  }

  @Override
  public Map<Integer, Routing> expectedRoutes() {
    Map<Integer, Routing> routingMap = new HashMap<>();
    // we are going to send to all tasks
    List<Integer> down = new ArrayList<>();
    down.addAll(destList);
    routingMap.put(task, new Routing(down));

    return routingMap;
  }

  @Override
  public void routeMessage(MessageHeader message, List<Integer> routes) {
    int next = random.nextInt(destList.size());
    routes.add(next);
  }
}
