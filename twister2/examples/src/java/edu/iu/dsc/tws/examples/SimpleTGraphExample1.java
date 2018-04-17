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
package edu.iu.dsc.tws.examples;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.executiongraph.ExecutionGraph;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.graph.DataflowTaskGraphParser;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.tsched.FirstFit.FirstFitTaskScheduling;
import edu.iu.dsc.tws.tsched.RoundRobin.RoundRobinTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

/**
 * This is the task graph generation class with input and output files.
 * It will be extended further to submit the job to the executor...
 */

public class SimpleTGraphExample1 implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTGraphExample1.class.getName());

  private int taskGraphFlag = 1;
  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  private Status status;

  //For time being it is declared as TaskGraphMapper...!
  private Set<ITask> parsedTaskSet = new HashSet<>();
  private DataflowTaskGraphGenerator dataflowTaskGraphGenerator = null;
  private DataflowTaskGraphParser dataflowTaskGraphParser = null;
  private ExecutionGraph executionGraph = null;
  private TaskSchedulePlan taskSchedulePlan = null;

  /**
   * Init method to submit the task to the executor
   */
  public void init(Config cfg, int containerId, ResourcePlan plan) {

    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    taskExecutor = new TaskExecutorFixedThread();
    this.status = Status.INIT;

    TaskPlan taskPlan = Utils.createTaskPlan(cfg, plan);
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);
    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    sources.add(0);
    int destination = 1;

    Map<String, Object> newCfg = new HashMap<>();
    LinkedQueue<IMessage> pongQueue = new LinkedQueue<IMessage>();
    taskExecutor.registerQueue(0, pongQueue);

    direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
        destination, new SimpleTGraphExample1.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    TaskMapper taskMapper = new TaskMapper("task1");
    TaskReducer taskReducer = new TaskReducer("task2");
    TaskShuffler taskShuffler = new TaskShuffler("task3");
    TaskMerger taskMerger = new TaskMerger("task4");

    if (containerId == 0) {
      WorkerPlan workerPlan = new WorkerPlan();
      Worker worker1 = new Worker(0);
      worker1.setCpu(4);
      worker1.setDisk(100);
      worker1.setRam(1024);

      Worker worker2 = new Worker(1);
      worker1.setCpu(4);
      worker1.setDisk(100);
      worker1.setRam(1024);
      workerPlan.addWorker(worker1);
      workerPlan.addWorker(worker2);

      GraphBuilder graphBuilder = GraphBuilder.newBuilder();
      graphBuilder.addTask("task1", taskMapper);
      graphBuilder.addTask("task2", taskReducer);
      graphBuilder.addTask("task3", taskShuffler);
      graphBuilder.addTask("task4", taskMerger);

      graphBuilder.connect("task1", "task2", "Reduce");
      graphBuilder.connect("task2", "task3", "Shuffle");
      graphBuilder.connect("task2", "task4", "merger1");
      graphBuilder.connect("task3", "task4", "merger2");

      graphBuilder.setParallelism("task1", 2);
      graphBuilder.setParallelism("task2", 2);
      graphBuilder.setParallelism("task3", 2);
      graphBuilder.setParallelism("task4", 1);

      graphBuilder.addConfiguration("task1", "Ram", 1024);
      graphBuilder.addConfiguration("task1", "Disk", 1000);
      graphBuilder.addConfiguration("task1", "Cpu", 2);

      graphBuilder.addConfiguration("task2", "Ram", 1024);
      graphBuilder.addConfiguration("task2", "Disk", 1000);
      graphBuilder.addConfiguration("task2", "Cpu", 2);

      graphBuilder.addConfiguration("task4", "Ram", 1024);
      graphBuilder.addConfiguration("task3", "Disk", 1000);
      graphBuilder.addConfiguration("task3", "Cpu", 2);

      graphBuilder.addConfiguration("task3", "Ram", 1024);
      graphBuilder.addConfiguration("task4", "Disk", 1000);
      graphBuilder.addConfiguration("task4", "Cpu", 2);

      DataFlowTaskGraph dataFlowTaskGraph = graphBuilder.build();
      LOG.info("Generated Dataflow Task Graph Is:" + dataFlowTaskGraph.getTaskVertexSet());

      if (containerId == 0) { //For running the task scheduling once
        //String schedulingMode = "RoundRobin";
        String schedulingMode = "FirstFit";
        if (dataFlowTaskGraph != null) {
          //if (cfg.get("SchedulingMode").equals("Round Robin")) {

          if ("RoundRobin".equals(schedulingMode)) {
            RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
            roundRobinTaskScheduling.initialize(cfg);
            taskSchedulePlan = roundRobinTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);
          } else if ("FirstFit".equals(schedulingMode)) {
            FirstFitTaskScheduling firstFitTaskScheduling = new FirstFitTaskScheduling();
            firstFitTaskScheduling.initialize(cfg);
            taskSchedulePlan = firstFitTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);
          }

          try {
            if (taskSchedulePlan.getContainersMap() != null) {
              LOG.info("Task schedule plan details:" + taskSchedulePlan.getContainersMap()
                  + "\t" + "Task Instance Size:"
                  + taskSchedulePlan.getContainersMap().keySet().iterator());
            }
          } catch (NullPointerException ne) {
            ne.printStackTrace();
          }
        }
      }
    }//End of ContainerId validation
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int[] d = new int[10];
    for (int i = 0; i < 10; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private class TaskMapper implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMapper(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, OutputCollection collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }

    /**
     * Assign the task name
     */
    @Override
    public String taskName() {
      return taskName;
    }
  }

  private class TaskReducer implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskReducer(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, OutputCollection collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }

    /**
     * Assign the task name
     */
    @Override
    public String taskName() {
      return taskName;
    }
  }

  private class TaskShuffler implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskShuffler(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, OutputCollection collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }

    /**
     * Assign the task name
     */
    @Override
    public String taskName() {
      return taskName;
    }
  }

  private class TaskMerger implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMerger(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, OutputCollection collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }

    /**
     * Assign the task name
     */
    @Override
    public String taskName() {
      return taskName;
    }
  }

  private class PingPongReceive implements MessageReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;
      if (count % 10000 == 0) {
        LOG.info("received message: " + count);
      }
      if (count == 100000) {
        status = Status.LOAD_RECEIVE_FINISHED;
      }
      return true;
    }

    @Override
    public void progress() {

    }
  }
}

