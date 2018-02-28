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

import java.util.ArrayList;
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
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowOperation;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphParser;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskGraphMapper;

/**
 * This is the task graph generation class with input and output files.
 * It will be extended further to submit the job to the executor...
 */

public class SimpleTGraph implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTGraph.class.getName());

  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  private Set<Task> parsedTaskSet;

  //to call the dataflow task graph generator
  private DataflowTaskGraphGenerator dataflowTaskGraphGenerator = null;
  private DataflowTaskGraphParser dataflowTaskGraphParser = null;
  private Status status;

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
        destination, new SimpleTGraph.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    //For Dataflow Task Graph Generation call the dataflow task graph generator
    //MapWorker sourceTask = new MapWorker(0, direct);
    //ReceiveWorker sinkTask = new ReceiveWorker();

    TMapper tMapper = new TMapper("1");
    TReducer tReducer = new TReducer("2");
    TShuffler tShuffler = new TShuffler("3");

    //Add the real input data files in the array list...
    tMapper.addInputData("mapper1", new ArrayList<>());
    tMapper.addInputData("reducer1", new ArrayList<>());

    //Mention the output data files to be generated in the array list...
    tMapper.addOutputData("mapperOut1", new ArrayList<>());
    tMapper.addOutputData("mapperOut2", new ArrayList<>());

    dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator()
        .generateTGraph(tMapper, tShuffler, new DataflowOperation("Map"))
        .generateTGraph(tShuffler, tReducer, new DataflowOperation("Shuffle"));

    LOG.info("Generated Dataflow Task Graph Vertices:"
        + dataflowTaskGraphGenerator.getTGraph().getTaskVertexSet());

    LOG.info("Generated Dataflow Task Edges:"
        + dataflowTaskGraphGenerator.getTGraph().getAllTaskEdges(tMapper, tShuffler).toString());

    LOG.info("Generated Dataflow Task Edges:"
        + dataflowTaskGraphGenerator.getTGraph().getAllTaskEdges(tShuffler, tReducer).toString());

    if (containerId == 0) {

      Thread mapThread = new Thread(new TMapper("1"));
      LOG.log(Level.INFO, "Starting map thread");
      mapThread.start();

      // we need to progress the communication
      while (true) {
        // progress the channel
        channel.progress();
        // we should progress the communication directive
        direct.progress();
        Thread.yield();
      }
    } else if (containerId == 1) {
      while (status != Status.LOAD_RECEIVE_FINISHED) {
        channel.progress();
        direct.progress();
      }
    }

    /*if (dataflowTaskGraphGenerator != null) {
      dataflowTaskGraphParser = new DataflowTaskGraphParser(dataflowTaskGraphGenerator);
      parsedTaskSet = dataflowTaskGraphParser.dataflowTaskGraphParseAndSchedule();
    }
    if (!parsedTaskSet.isEmpty()) {
      if (containerId == 0) {
        LOG.info("Job in if loop is::::::::::::" + parsedTaskSet.iterator().next());
        taskExecutor.registerTask(parsedTaskSet.iterator().next());
        //taskExecutor.registerTask(new MapWorker(0, direct));
        taskExecutor.submitTask(0);
        taskExecutor.progres();
        ///dataflowTaskGraphGenerator.removeTaskVertex(parsedTaskSet.iterator().next());
      } else if (containerId == 1) {
        int index = 0;
        for (Task processedTask : parsedTaskSet) {
          if (index == 0) {
            ++index;
          } else if (index == 1) {
            LOG.info("Job in else loop is::::::::::::" + processedTask);
            ArrayList<Integer> inq = new ArrayList<>();
            inq.add(0);
            taskExecutor.setTaskMessageProcessLimit(10000);
            taskExecutor.registerSinkTask(processedTask, inq);
            taskExecutor.progres();
            ///dataflowTaskGraphGenerator.removeTaskVertex(parsedTaskSet.iterator().next());
            ++index;
          } else if (index > 1) { //Just for verification
            LOG.info("Task Index is greater than 1");
            LOG.info("Submit the job to pipeline task");
            break;
          }
        }
      }
    }*/
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

  private class TMapper extends TaskGraphMapper implements Runnable {

    protected TMapper(String taskId) {
      super(taskId);
    }

    @Override
    public void execute() {
      System.out.println("&&&& Task Graph Map Function with Input and Output Files &&&&");
      for (int i = 0; i < 10; i++) { //100000
        IntData data = generateData();
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Thread.yield();
    }
  }

  private class TShuffler extends TaskGraphMapper implements Runnable {

    protected TShuffler(String taskId) {
      super(taskId);
    }

    @Override
    public void execute() {
      System.out.println("&&&& Task Graph Shuffle Function with Input and Output Files &&&&");
      for (int i = 0; i < 100000; i++) { //100000
        IntData data = generateData();
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Thread.yield();
    }
  }

  private class TReducer extends TaskGraphMapper implements Runnable {

    protected TReducer(String taskId) {
      super(taskId);
    }

    @Override
    public void execute() {
      System.out.println("&&& Task Graph Reduce Function with Input and Output Files &&&");
      for (int i = 0; i < 10; i++) { //100000
        IntData data = generateData();
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Thread.yield();
    }
  }

  private class PingPongReceive implements MessageReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
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






