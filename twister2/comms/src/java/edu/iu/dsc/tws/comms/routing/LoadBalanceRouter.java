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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class LoadBalanceRouter implements IRouter {
  private Config config;
  private TaskPlan taskPlan;
  private Set<Integer> sources;
  private int stream;
  private List<Integer> destList;
  private Random random;

  public LoadBalanceRouter(Config cfg, TaskPlan plan,
                   Set<Integer> srscs, Set<Integer> dests,
                   int strm, int distinctRoutes) {
    this.config = cfg;
    this.taskPlan = plan;
    this.sources = srscs;
    this.stream = strm;
    random = new Random(System.nanoTime());

    destList = new ArrayList<>(dests);
  }

  @Override
  public Set<Integer> receivingExecutors() {
    return null;
  }

  @Override
  public Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return null;
  }

  @Override
  public boolean isLastReceiver() {
    return false;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int src) {
    return null;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source) {
    return null;
  }

  @Override
  public int mainTaskOfExecutor(int executor) {
    return 0;
  }

  @Override
  public int destinationIdentifier(int source, int path) {
    return 0;
  }
}
