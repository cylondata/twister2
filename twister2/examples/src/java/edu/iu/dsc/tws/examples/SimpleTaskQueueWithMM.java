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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Longs;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.MemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;

public class SimpleTaskQueueWithMM implements IContainer {
  private static final Logger LOG = Logger.getLogger(SimpleTaskQueueWithMM.
      class.getName());

  private DataFlowOperation direct;

  private TaskExecutorFixedThread taskExecutor;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  /**
   * Initialize the container
   */
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());
    //Creates task an task executor instance to be used in this container
    taskExecutor = new TaskExecutorFixedThread();
    this.status = Status.INIT;

    // lets create the task plan
    TaskPlan taskPlan = Utils.createTaskPlan(cfg, plan);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    // we are sending messages from 0th task to 1st task
    Set<Integer> sources = new HashSet<>();
    sources.add(0);
    int dests = 1;
    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");

    Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase");
    MemoryManager memoryManager = new LMDBMemoryManager(dataPath);


    // this method calls the init method
    // I think this is wrong
    //TODO: Does the task genereate the communication or is it done by a controller for examples
    // the direct comm between task 0 and 1 is it done by the container or the the task

    //TODO: if the task creates the dataflowop does the task progress it or the executor

    //TODO : FOR NOW the dataflowop is created at container and sent to task
    LinkedQueue<Message> pongQueue = new LinkedQueue<Message>();
    taskExecutor.registerQueue(0, pongQueue);

    direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
        dests, new PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    //Memory Manager
    if (containerId == 0) {
      byte[] val = Longs.toByteArray(1231212121213L);
      byte[] val2 = Longs.toByteArray(22222222L);
      ByteBuffer valbuf = ByteBuffer.allocateDirect(8192);
      memoryManager.put(0, "temp", valbuf);
//      memoryManager.put(0, "temp", val);
//      memoryManager.put(0, "temp", val2);
      // the map thread where data is produced
//      LOG.log(Level.INFO, "Starting map thread");
//      SourceTask<Object> mapTask = new MapWorker(0, direct);
//      mapTask.setMemoryManager(memoryManager);
//      taskExecutor.registerTask(mapTask);
//      taskExecutor.submitTask(0);
//      taskExecutor.progres();

    } else if (containerId == 1) {
      byte[] val3 = Longs.toByteArray(3333333L);
      ByteBuffer val3buf = ByteBuffer.wrap(val3);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      ByteBuffer results = memoryManager.get(0, "temp");
      if (results.limit() == 8192) {
        System.out.println("Correct " + results.limit());
      }

      ByteBuffer valbuf2 = ByteBuffer.allocateDirect(16192);
      memoryManager.put(0, "temp", valbuf2);
      results = memoryManager.get(0, "temp");
      if (results.limit() == 16192) {
        System.out.println("Correct " + results.limit());
      }


      ByteBuffer results2 = memoryManager.get(0, "temp");

      ByteBuffer results3 = memoryManager.get(0, "temp");
      if (results2 == null) {

        System.out.println("Missing key is null");
      }
      if (results3.getLong() == 1231212121213L) {
        System.out.println("Long value is correct");
      }

      memoryManager.append(0, "temp", val3buf);

      ByteBuffer resultsappend = memoryManager.get(0, "temp");
      System.out.println("Long value 1 :" + resultsappend.getLong());
      System.out.println("Long value 1 :" + resultsappend.getLong());

//      ArrayList<Integer> inq = new ArrayList<>();
//      inq.add(0);
//      taskExecutor.setTaskMessageProcessLimit(10000);
//      SinkTask<Object> recTask = new RecieveWorker(1);
//      recTask.setMemoryManager(memoryManager);
//      taskExecutor.registerSinkTask(recTask, inq);
//      taskExecutor.progres();
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
      if (count % 50000 == 0) {
        LOG.info("received message: " + count);
      }
      taskExecutor.submitMessage(0, "" + count);

      if (count == 10) {
        status = Status.LOAD_RECEIVE_FINISHED;
      }
      return true;
    }

    @Override
    public void progress() {

    }
  }

  /**
   * RevieceWorker
   */
  private class RecieveWorker extends SinkTask<Object> {

    RecieveWorker(int tid) {
      super(tid);
    }

    @Override
    public Message execute() {
      return null;
    }

    @Override
    public Message execute(Message content) {
      try {
        // Sleep for a while
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      String data = content.getContent().toString();
      if (Integer.parseInt(data) % 1000 == 0) {
        long keytemp = 1234L;
        byte[] init = Longs.toByteArray(1L);
        long val = 1L;
        if (getMemoryManager().containsKey(0, "temp")) {
//          byte[] temp = getMemoryManager().getBytes(0, keytemp);
//          final ByteBuffer valBuffer = allocateDirect(Long.BYTES);
//          valBuffer.put(temp, 0, temp.length);
//          val = valBuffer.getLong();
//          val = val + 1;
//          init = Longs.toByteArray(val);
        }
        //getMemoryManager().put(0, "temp", init);
        System.out.println(((String) content.getContent()).toString()
            + " Value of mapped long : " + val);
      }
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
      for (int i = 0; i < 100000; i++) {
        IntData data = generateData();
        // lets generate a message

        while (!getDataFlowOperation().send(0, data, 0)) {
          // lets wait a litte and try again
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
}
