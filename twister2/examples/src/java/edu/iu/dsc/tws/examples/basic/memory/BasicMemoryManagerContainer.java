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
package edu.iu.dsc.tws.examples.basic.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Ints;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KeyedContent;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.MemoryManager;
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BasicMemoryManagerContainer implements IContainer {
  private static final Logger LOG = Logger.getLogger(BasicMemoryManagerContainer.class.getName());

  private DataFlowOperation aggregate;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 2;

  private int noOfTasksPerExecutor = 2;

  private RandomString randomString;

  private long startTime = 0;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;
    this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();
    this.randomString = new RandomString(128000, new Random(), RandomString.ALPHANUM);

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();
    LOG.info("###################### Running LMDB unit tests ######################");
    testPrimitives();

//    try {
//      // this method calls the init method
//      // I think this is wrong
//
//      aggregate = channel.gather(newCfg, MessageType.OBJECT, MessageType.INTEGER,  0, sources,
//          dest, new FinalGatherReceive());
//
////      aggregate = channel.gather(newCfg, MessageType.OBJECT, 0, sources,
////          dest, new FinalGatherReceive());
//
//      for (int i = 0; i < noOfTasksPerExecutor; i++) {
//        // the map thread where data is produced
//        LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
//        Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
//        mapThread.start();
//      }
//      // we need to progress the communication
//      while (true) {
//        try {
//          // progress the channel
//          channel.progress();
//          // we should progress the communication directive
//          aggregate.progress();
//          Thread.yield();
//        } catch (Throwable t) {
//          t.printStackTrace();
//        }
//      }
//    } catch (Throwable t) {
//      t.printStackTrace();
//    }
  }

  /**
   * test primitives with memory manager
   */
  public boolean testPrimitives() {
    LOG.info("## Running LMDB primitives test ##");

    boolean allPassed = true;
    Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase");
    MemoryManager memoryManager = new LMDBMemoryManager(dataPath);
    int opID = 1;
    OperationMemoryManager op = memoryManager.addOperation(opID, DataMessageType.INTEGER);

    //Test single integer operation
    ByteBuffer key = ByteBuffer.allocateDirect(4);
    ByteBuffer value = ByteBuffer.allocateDirect(4);
    key.putInt(1);
    int testInt = 1231212121;
    byte[] val = Ints.toByteArray(testInt);
    value.put(val);
    op.put(key, value);

    ByteBuffer results = op.get(key);
    int res = results.getInt();
    if (res != testInt) {
      allPassed = false;
    }

    if (allPassed) {
      System.out.println("Passed int test");
    }
    //test int array, put should replace the current value
    int[] testarray = {234, 14123, 534, 6345};
    value = ByteBuffer.allocateDirect(16);
    for (int i : testarray) {
      value.putInt(i);
    }

    op.put(key, value);
    results = op.get(key);
    for (int i : testarray) {
      if (i != results.getInt()) {
        allPassed = false;
      }
    }
    if (allPassed) {
      System.out.println("Passed int array test");
    }

    // get retuls with iterator
    Iterator<Object> iter = op.iterator();
    int[] dataset = (int[]) iter.next();
    for (int i = 0; i < dataset.length; i++) {
      if (dataset[i] != testarray[i]) {
        allPassed = false;
      }
    }
    if (allPassed) {
      System.out.println("Passed int array iterator test, number of values returned by"
          + "iterator : " + 1);
    }
    // iterator with more than 1 key will test that keys are sorted properly
    int[][] datamultiarray = {{1, 11, 111, 1111}, {2, 22, 222, 2222}, {3, 33, 333, 3333},
        {4, 44, 444, 4444}};
    key.clear();
    value.clear();
    key.putInt(4);
    for (int i : datamultiarray[3]) {
      value.putInt(i);
    }
    op.put(key, value);

    key.clear();
    value.clear();
    key.putInt(1);
    for (int i : datamultiarray[0]) {
      value.putInt(i);
    }
    op.put(key, value);

    key.clear();
    value.clear();
    key.putInt(3);
    for (int i : datamultiarray[2]) {
      value.putInt(i);
    }
    op.put(key, value);

    key.clear();
    value.clear();
    key.putInt(2);
    for (int i : datamultiarray[1]) {
      value.putInt(i);
    }
    op.put(key, value);

    Iterator<Object> itermulti = op.iterator();
    int itercount = 0;
    itercount = 0;
    while (itermulti.hasNext()) {
      dataset = (int[]) itermulti.next();
      for (int i = 0; i < 4; i++) {
        if (dataset[i] != datamultiarray[itercount][i]) {
          allPassed = false;
        }
      }
      itercount++;
    }

    if (allPassed) {
      System.out.println("Passed int multi array iterator test, number of values returned by"
          + "iterator : " + itercount);
    }

    return allPassed;
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int task = 0;
    private int sendCount = 0;

    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
//      MPIBuffer data = new MPIBuffer(1024);
        startTime = System.nanoTime();
        for (int i = 0; i < 1; i++) {
          String data = generateStringData();
          // lets generate a message
          KeyedContent mesage = new KeyedContent(task, data,
              MessageType.INTEGER, MessageType.OBJECT);
//
          while (!aggregate.send(task, mesage, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
//          LOG.info(String.format("%d sending to %d", id, task)
//              + " count: " + sendCount++);
//          if (i % 10 == 0) {
//            LOG.info(String.format("%d sent %d", id, i));
//          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalGatherReceive implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      // add the object to the map
      boolean canAdd = true;
      if (count == 0) {
        start = System.nanoTime();
      }

      try {
        List<Object> m = messages.get(target).get(source);
        if (messages.get(target) == null) {
          throw new RuntimeException(String.format("%d Partial receive error %d", id, target));
        }
        Integer c = counts.get(target).get(source);
        if (m.size() > 16) {
          LOG.info(String.format("%d Final true: target %d source %d %s",
              id, target, source, counts));
          canAdd = false;
        } else {
          LOG.info(String.format("%d Final false: target %d source %d %s",
              id, target, source, counts));
          m.add(object);
          counts.get(target).put(source, c + 1);
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public void progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
          Map<Integer, Integer> cMap = counts.get(t);
          boolean found = true;
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            } else {
              o = e.getValue().get(0);
            }
          }
          if (found) {
            for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
              o = e.getValue().remove(0);
            }
            for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
              Integer i = e.getValue();
              cMap.put(e.getKey(), i - 1);
            }
            if (o != null) {
              count++;
              if (count % 1 == 0) {
                LOG.info(String.format("%d Last %d count: %d %s",
                    id, t, count, counts));
              }
              if (count >= 1) {
                LOG.info("Total time: " + (System.nanoTime() - start) / 1000000
                    + " Count: " + count + " total: " + (System.nanoTime() - startTime));
              }
            } else {
              LOG.severe("We cannot find an object and this is not correct");
            }
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
    int s = 128000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  private String generateStringData() {
    return randomString.nextString();
  }
}

