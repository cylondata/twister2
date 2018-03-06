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
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;

import edu.iu.dsc.tws.task.executiongraph.ExecutionGraph;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowOperation;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTGraphParser;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskGraphMapper;

/**
 * This is the task graph generation class with input and output files.
 * It will be extended further to submit the job to the executor...
 */

public class SimpleTGraphExample implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTGraphExample.class.getName());
  private int taskGraphFlag = 1;
  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  //For time being it is declared as TaskGraphMapper...!
  private Set<TaskGraphMapper> parsedTaskSet;
  //to call the dataflow task graph generator
  private DataflowTaskGraphGenerator dataflowTaskGraphGenerator = null;
  private DataflowTGraphParser dataflowTGraphParser = null;
  private Status status;

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
        destination, new SimpleTGraphExample.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    TMapper tMapper = new TMapper("1");
    TReducer tReducer = new TReducer("2");
    TShuffler tShuffler = new TShuffler("3");
    TReducer tMergeFinal = new TReducer("4");

    //Add the real input data files in the array list...
    tMapper.addInputData("mapper1", new ArrayList<>());
    tMapper.addInputData("mapper2", new ArrayList<>());

    //Add the real input data files in the array list...
    tReducer.addInputData("reducer1", new ArrayList<>());
    tReducer.addInputData("reducer2", new ArrayList<>());

    //Add the real input data files in the array list...
    tShuffler.addInputData("shuffler1", new ArrayList<>());
    tShuffler.addInputData("shuffler2", new ArrayList<>());

    //Add the real input data files in the array list...
    tMergeFinal.addInputData("merge1", new ArrayList<>());
    tMergeFinal.addInputData("merge2", new ArrayList<>());

    //Mention the output data files to be generated in the array list...
    tMapper.addOutputData("mapperOut1", new ArrayList<>());
    tMapper.addOutputData("mapperOut2", new ArrayList<>());

    if (taskGraphFlag >= 0) { //just for verification (replace with proper value)
      /*dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator()
          .generateTGraph(tMapper, tShuffler, new DataflowOperation("Map"))
          .generateTGraph(tMapper, tReducer, new DataflowOperation("Shuffle"))
          .generateTGraph(tShuffler, tReducerFinal, new DataflowOperation("finalReduce"))
          .generateTGraph(tReducer, tReducerFinal, new DataflowOperation("finalReduce"));*/

      dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator()
          .generateTGraph(tMapper)
          .generateTGraph(tMapper, tReducer, new DataflowOperation("Reduce"))
          .generateTGraph(tMapper, tShuffler, new DataflowOperation("Shuffle"))
          .generateTGraph(tReducer, tMergeFinal, new DataflowOperation("Merge1"))
          .generateTGraph(tShuffler, tMergeFinal, new DataflowOperation("Merge2"));

      LOG.info("Generated Dataflow Task Graph Vertices:"
          + dataflowTaskGraphGenerator.getTGraph().getTaskVertexSet());


      if (dataflowTaskGraphGenerator != null) {
        dataflowTGraphParser = new DataflowTGraphParser(dataflowTaskGraphGenerator);
        parsedTaskSet = dataflowTGraphParser.dataflowTGraphParseAndSchedule();
        LOG.info("parsed task set:" + parsedTaskSet);
      }
      //parsedTaskSet = executionGraph.parseTaskGraph(dataflowTaskGraphGenerator);
      if (!parsedTaskSet.isEmpty()) {
        //newly added for testing
        executionGraph = new ExecutionGraph(parsedTaskSet);
        String message = executionGraph.generateExecutionGraph(containerId);
        //String message = executionGraph.generateExecutionGraph(containerId, parsedTaskSet);
        /*TaskExecutorFixedThread taskExecutionGraph =
            executionGraph.generateExecutionGraph(containerId, parsedTaskSet);*/
        LOG.info(message);
      }
    }

    //It removes only the first tax vertex in the parsedTaskSet.
    //dataflowTaskGraphGenerator.removeTaskVertex(parsedTaskSet.iterator().next());
    //It is getting concurrent modification exception...!
    /*for (TaskGraphMapper processedTask : parsedTaskSet) {
      dataflowTaskGraphGenerator.removeTaskVertex(processedTask);
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
          System.out.println(i);
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
      for (int i = 0; i < 10; i++) { //100000
        IntData data = generateData();
        try {
          System.out.println(i);
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
          System.out.println(i);
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
