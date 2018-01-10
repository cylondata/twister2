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
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphParser;

public class SimpleCxMultiTaskGraph implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTaskGraph.class.getName());

  private DataFlowOperation direct;
  private DataFlowOperation direct1;
  private TaskExecutorFixedThread taskExecutor;
  private Set<Task> parsedTaskSet;

  //to call the dataflow task graph generator
  private DataflowTaskGraphGenerator dataflowTaskGraph = null;
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
        destination, new SimpleCxMultiTaskGraph.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    direct1 = channel.direct(newCfg, MessageType.OBJECT, 1, sources,
        destination, new SimpleCxMultiTaskGraph.PingPongReceive());
    taskExecutor.initCommunication(channel, direct1);

    //For Dataflow Task Graph Generation call the dataflow task graph generator
    MapWorker sourceTask = new MapWorker(0, direct);
    ReceiveWorker sinkTask = new ReceiveWorker();
    ReceiveWorker sinkTask1 = new ReceiveWorker();

    //task 0 -> task 1 direct communication channel
    //task 0 -> task 2 create different task edge or communication channel
    dataflowTaskGraph = new DataflowTaskGraphGenerator()
        .generateDataflowGraph(sourceTask, sinkTask, direct)
        .generateDataflowGraph(sourceTask, sinkTask1, direct1);

    if (dataflowTaskGraph != null) {
      dataflowTaskGraphParser = new DataflowTaskGraphParser(dataflowTaskGraph);
      parsedTaskSet = dataflowTaskGraphParser.dataflowTaskGraphParseAndSchedule();
    }

    if (!parsedTaskSet.isEmpty()) {
      if (containerId == 0) {
        LOG.log(Level.INFO, "Job In If Loop" + parsedTaskSet.iterator().next());
        taskExecutor.registerTask(parsedTaskSet.iterator().next());
        taskExecutor.submitTask(0);
        taskExecutor.progres();
      } else if (containerId >= 1) { //This loop should be modified for the complex task graphs
        int index = 0;
        for (Task processedTask : parsedTaskSet) {
          if (index == 0) {
            ++index;
          } else if (index == 1) {
            ArrayList<Integer> inq = new ArrayList<>();
            inq.add(0);
            taskExecutor.setTaskMessageProcessLimit(10000);
            taskExecutor.registerSinkTask(processedTask, inq);
            taskExecutor.progres();
            ++index;
          } else if (index > 1) {
            ArrayList<Integer> inq1 = new ArrayList<>();
            inq1.add(0);
            taskExecutor.setTaskMessageProcessLimit(10000);
            taskExecutor.registerSinkTask(processedTask, inq1);
            taskExecutor.progres();
            ++index;
          } else if (index > 2) {
            //it would be constructed based on the container value and no.of tasks
            LOG.info("Task Index is greater than 2");
            break;
          }
        }
      }
    }
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int[] d = new int[10];
    LOG.info("I am in generate data method");
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

  private class ReceiveWorker extends SinkTask<Object> {

    @Override
    public Message execute() {
      return null;
    }

    @Override
    public Message execute(Message content) {
      return null;
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker extends SourceTask<Object> {
    private int sendCount = 0;

    MapWorker(int tid, DataFlowOperation dataFlowOperation) {
      super(tid, dataFlowOperation);
    }

    @Override
    public Message execute() {
      LOG.log(Level.INFO, "Starting map worker");
      for (int i = 0; i < 10000; i++) { //100000
        IntData data = generateData();
        while (!getDataFlowOperation().send(0, data, 0)) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        sendCount++;
        Thread.yield();
      }
      status = Status.MAP_FINISHED;
      return null;
    }

    @Override
    public Message execute(Message content) {
      return execute();
    }
  }
}




