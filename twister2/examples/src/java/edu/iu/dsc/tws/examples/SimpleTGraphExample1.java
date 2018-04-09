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
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.executiongraph.ExecutionGraph;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphParser;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskEdge;
import edu.iu.dsc.tws.tsched.FirstFit.FirstFitTaskScheduling;
import edu.iu.dsc.tws.tsched.RoundRobin.RoundRobinTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.common.TaskConfig;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.Task;

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
  private Set<ITask> parsedTaskSet;
  private DataflowTaskGraphGenerator dataflowTaskGraphGenerator = null;
  private DataflowTaskGraphParser dataflowTaskGraphParser = null;
  private ExecutionGraph executionGraph = null;

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
    LinkedQueue<Message> pongQueue = new LinkedQueue<Message>();
    taskExecutor.registerQueue(0, pongQueue);

    direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
        destination, new SimpleTGraphExample1.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    TaskMapper taskMapper = new TaskMapper("task1");
    TaskReducer taskReducer = new TaskReducer("task2");
    TaskShuffler taskShuffler = new TaskShuffler("task3");
    TaskMerger taskMerger = new TaskMerger("task4");
    if (containerId == 0) {
      if (taskGraphFlag >= 0) { //just for verification (replace with proper value)
        dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator()
            .generateTaskGraph(taskMapper)
            .generateTaskGraph(taskMapper, taskReducer, new TaskEdge("Reduce"))
            .generateTaskGraph(taskMapper, taskShuffler, new TaskEdge("Shuffle"))
            .generateTaskGraph(taskReducer, taskMerger, new TaskEdge("Merge1"))
            .generateTaskGraph(taskShuffler, taskMerger, new TaskEdge("Merge2"));

        LOG.info("Generated Dataflow Task Graph Vertices:"
            + dataflowTaskGraphGenerator.getTaskgraph().getTaskVertexSet());

        if (dataflowTaskGraphGenerator != null) {

          //For testing to process the child task source & target vertices
          dataflowTaskGraphGenerator.getDataflowTaskChildTasks(dataflowTaskGraphGenerator);
          /////////////////////////////

          dataflowTaskGraphParser = new DataflowTaskGraphParser(dataflowTaskGraphGenerator);
          parsedTaskSet = dataflowTaskGraphParser.taskGraphParseAndSchedule();

          Job job = new Job();
          job.setJobId(1);
          job.setTaskLength(parsedTaskSet.size());
          job.setJob(job);
          Task[] taskList = new Task[job.getTaskLength()];
          //Task[] taskList = new Task[parsedTaskSet.size()];
          int i = 0;
          for (ITask processedTask : parsedTaskSet) {
            Task task = new Task();
            task.setTaskName(String.format("task%d", i));
            task.setTaskCount(2);
            task.setRequiredRam(1024.0);
            task.setRequiredDisk(100.0);
            task.setRequiredCpu(10.0);
            taskList[i] = task;
            i++;
          }
          job.setTasklist(taskList);

          TaskSchedulePlan taskSchedulePlan = null;
          if (TaskConfig.schedulingMode.equals("Round Robin")) {
            RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
            roundRobinTaskScheduling.initialize(job);
            taskSchedulePlan = roundRobinTaskScheduling.tschedule();
          } else if (TaskConfig.schedulingMode.equals("First Fit")) {
            FirstFitTaskScheduling firstFitTaskScheduling = new FirstFitTaskScheduling();
            firstFitTaskScheduling.initialize(job);
            taskSchedulePlan = firstFitTaskScheduling.tschedule();
          }
          if (taskSchedulePlan.getContainersMap() != null) {
            System.out.println("Task schedule plan details:" + taskSchedulePlan.getContainersMap());
          }
        }
      }

      //parsedTaskSet = executionGraph.parseTaskGraph(dataflowTaskGraphGenerator);
      /*if (!parsedTaskSet.isEmpty()) {
        executionGraph = new ExecutionGraph(parsedTaskSet);
        String message = executionGraph.generateExecutionGraph(containerId);
        String message = executionGraph.generateExecutionGraph(containerId, parsedTaskSet);
        TaskExecutorFixedThread taskExecutionGraph =
            executionGraph.generateExecutionGraph(containerId, parsedTaskSet);
        LOG.info(message);
      }*/
    }
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
    public Message execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public Message execute(Message content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(Message content) {

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
    public Message execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public Message execute(Message content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(Message content) {

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
    public Message execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public Message execute(Message content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(Message content) {

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
    public Message execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public Message execute(Message content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(Message content) {

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
