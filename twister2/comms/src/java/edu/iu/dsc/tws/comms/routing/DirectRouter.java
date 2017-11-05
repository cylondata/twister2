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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.core.TaskPlan;

public class DirectRouter implements IRouter {
  private static final Logger LOG = Logger.getLogger(DirectRouter.class.getName());

  private int destination;
  private TaskPlan taskPlan;
  private HashSet<Integer> downStream;
  private Map<Integer, List<Integer>> upstream;
  private Set<Integer> receiveExecutors;
  private Set<Integer> thisExecutorTasks;

  public DirectRouter(TaskPlan plan, Set<Integer> srscs, int dest) {
    this.destination = dest;
    this.taskPlan = plan;

    this.downStream = new HashSet<>();
    this.downStream.add(dest);

    this.upstream = new HashMap<>();
    List<Integer> sources = new ArrayList<>();
    sources.addAll(srscs);
    this.upstream.put(0, sources);

    int destinationExecutor = executor(destination);
    receiveExecutors = new HashSet<>();
    if (destinationExecutor == taskPlan.getThisExecutor()) {
      for (int s : srscs) {
        int e = executor(s);
        if (destinationExecutor != e) {
          receiveExecutors.add(e);
        }
      }
    }
    this.thisExecutorTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
  }

  @Override
  public Set<Integer> receivingExecutors() {
    LOG.info(taskPlan.getThisExecutor() + " Receiving executors: " + receiveExecutors);
    return receiveExecutors;
  }

  @Override
  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    // check if this executor contains
    if (thisExecutorTasks.contains(destination)) {
      LOG.info(taskPlan.getThisExecutor() + " Receive expected tasks: " + upstream.get(0));
      return upstream;
    }

    return new HashMap<>();
  }

  @Override
  public boolean isLast(int task) {
    // now check if destination is in this task
    return thisExecutorTasks.contains(destination);
  }

  @Override
  public Set<Integer> getDownstreamTasks(int source) {
    // return a routing
    return downStream;
  }

  @Override
  public int executor(int task) {
    return taskPlan.getExecutorForChannel(task);
  }

  @Override
  public int mainTaskOfExecutor(int executor) {
    return 0;
  }

  @Override
  public int destinationIdentifier() {
    return 0;
  }
}
