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
package edu.iu.dsc.tws.executor;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.graph.BaseDataflowTaskGraph;
import edu.iu.dsc.tws.task.graph.TaskEdge;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class DefaultExecutor implements IExecutor {
  private static final Logger LOG = Logger.getLogger(DefaultExecutor.class.getName());

  /**
   * no of threads used for this executor
   */
  private int noOfThreads;

  /**
   * Worker id
   */
  private int workerId;

  public DefaultExecutor(int workerId) {
    this.workerId = workerId;
  }

  @Override
  public Execution schedule(BaseDataflowTaskGraph<ITask, TaskEdge> taskGraph,
                            TaskSchedulePlan taskSchedule) {
    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap = taskSchedule.getContainersMap();

    TaskSchedulePlan.ContainerPlan p = containersMap.get(workerId);

    if (p == null) {
      return null;
    }

    // lets get the task
    ITask task = null;

    // now lets get the outgoing edges of this task
    Set<String> outOperations = null;
    Set<String> inOperations = null;

    // now lets create the communications for these tasks
    return null;
  }

  @Override
  public void wait(Execution execution) {

  }

  @Override
  public void stop(Execution execution) {

  }
}
