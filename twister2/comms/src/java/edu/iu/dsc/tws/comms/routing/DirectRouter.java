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

import edu.iu.dsc.tws.comms.core.TaskPlan;

public class DirectRouter implements IRouter {
  private int destination;
  private TaskPlan taskPlan;
  private HashSet<Integer> downStream;
  private Map<Integer, List<Integer>> upstream;
  private Set<Integer> receiveExecutors;

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
    for (int s : srscs) {
      int e = executor(s);
      if (destinationExecutor != e) {
        receiveExecutors.add(e);
      }
    }
  }

  @Override
  public Set<Integer> receivingExecutors() {
    return receiveExecutors;
  }

  @Override
  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    // check if this executor contains
    if (isLast()) {
      return upstream;
    }

    return new HashMap<>();
  }

  @Override
  public boolean isLast() {
    Set<Integer> tasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
    // now check if destination is in this task
    return tasks.contains(destination);
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
}
