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
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphParser;

public class SimpleTaskGraph implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTaskGraph.class.getName());

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
        destination, new SimpleTaskGraph.PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    //For Dataflow Task Graph Generation call the dataflow task graph generator
    MapWorker sourceTask = new MapWorker(0, direct);
    ReceiveWorker sinkTask = new ReceiveWorker();

    dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator().generateDataflowGraph(
        sourceTask, sinkTask, direct);
    if (dataflowTaskGraphGenerator != null) {
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
    }

    //This scheduling loop will be used in the future, leave it for reference.
    /*int index = 0;
    if (!parsedTaskSet.isEmpty()) {
      for (Task processedTask : parsedTaskSet) {
        if (containerId == index) {
          taskExecutor.registerTask(processedTask);
          taskExecutor.submitTask(0);
          taskExecutor.progres();
          index++;
        } else if (index == 1) {
          ArrayList<Integer> inq = new ArrayList<>();
          inq.add(0);
          taskExecutor.setTaskMessageProcessLimit(10000);
          taskExecutor.registerSinkTask(processedTask, inq);
          taskExecutor.progres();
          index++;
        } else if(index > 1){
          List<Task> taskList = new ArrayList<>();
          for (Task processedTasks : parsedTaskSet) {
            taskList.add(processedTasks);
          }
          //This loop should be properly written...!
          taskExecutionOptimizer = new TaskExecutionOptimizer(taskExecutor);
          Map<Integer, List<Task>> taskMap = new HashMap<>();
          taskMap.put(1, taskList);
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
      for (int i = 0; i < 100000; i++) { //100000
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



