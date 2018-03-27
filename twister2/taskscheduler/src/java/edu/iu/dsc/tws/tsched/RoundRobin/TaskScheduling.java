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
package edu.iu.dsc.tws.tsched.RoundRobin;

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
//import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.Task;
import edu.iu.dsc.tws.tsched.utils.Utils;

public class TaskScheduling implements IContainer {

  private int taskGraphFlag = 1;
  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  private Status status;

  public void schedule() {
    System.out.println("I am inside init method");
    Job job = new Job();
    job.setJob(job);
    Task[] taskList = new Task[4];
    RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
    //Config config = null;
    //roundRobinTaskScheduling.initialize(config, job);
    roundRobinTaskScheduling.initialize(job);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduling.tschedule();
  }

  @Override
  public void init(edu.iu.dsc.tws.common.config.Config config, int id, ResourcePlan resourcePlan) {

    taskExecutor = new TaskExecutorFixedThread();
    this.status = Status.INIT;

    TaskPlan taskPlan = Utils.createTaskPlan(config, resourcePlan);
    TWSNetwork network = new TWSNetwork(config, taskPlan);
    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    sources.add(0);

    //schedule();
    try {
      Job job = new Job();
      job.setJobId(1);
      job.setTaskLength(4);
      job.setJob(job);

      RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
      roundRobinTaskScheduling.initialize(job);
      TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduling.tschedule();
      System.out.println("Task schedule plan details:" + taskSchedulePlan);
    } catch (Exception ee) {
      ee.printStackTrace();
    }
  }

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }
}

