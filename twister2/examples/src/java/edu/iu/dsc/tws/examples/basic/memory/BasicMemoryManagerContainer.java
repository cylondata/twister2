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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Ints;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.BufferedMemoryManager;
import edu.iu.dsc.tws.data.memory.MemoryManager;
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
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
    testPrimitivesLMDB();

    LOG.info("################# Running BufferedMemoryManager unit tests ##################");
    testPrimitivesBuffered();
  }

  /**
   * test primitives with LMDB memory manager
   */
  public boolean testPrimitivesLMDB() {
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
      System.out.println("Passed LMDB int test");
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
      System.out.println("Passed LMDB int array test");
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
      System.out.println("Passed LMDB int array iterator test, number of values returned by"
          + "iterator : " + 1);
    }
    // iterator with more than 1 key will test that keys are sorted properly
    int[][] datamultiarray = {{1, 11, 111, 1111}, {2, 22, 222, 2222}, {3, 33, 333, 3333},
        {4, 44, 444, 4444}};
    ByteBuffer value2 = ByteBuffer.allocateDirect(16);
    ByteBuffer value3 = ByteBuffer.allocateDirect(16);
    ByteBuffer value4 = ByteBuffer.allocateDirect(16);
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
      if (itercount > 3) {
        break;
      }
      dataset = (int[]) itermulti.next();
      for (int i = 0; i < 4; i++) {
        if (dataset[i] != datamultiarray[itercount][i]) {
          allPassed = false;
        }
      }
      itercount++;
    }

    if (allPassed) {
      System.out.println("Passed LMDB int multi array iterator test, number of values returned by"
          + "iterator : " + itercount);
    }


    //test append function
    key.clear();
    value.clear();
    key.putInt(6);
    for (int i : datamultiarray[0]) {
      value.putInt(i);
    }
    op.put(key, value);
    value.clear();
    for (int i : datamultiarray[1]) {
      value.putInt(i);
    }
    op.append(key, value);

    results = op.get(key);
    for (int i : datamultiarray[0]) {
      int itemp = results.getInt();
      if (i != itemp) {
        allPassed = false;
      }
    }
    for (int i : datamultiarray[1]) {
      int itemp = results.getInt();
      if (i != itemp) {
        allPassed = false;
      }
    }

    if (allPassed) {
      System.out.println("Passed LMDB int array append test");
    }

    return allPassed;
  }

  /**
   * test primitives with Buffered memory manager
   */
  public boolean testPrimitivesBuffered() {
    LOG.info("## Running BufferedMemoryManager primitives test ##");

    boolean allPassed = true;
    Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase2");
    MemoryManager memoryManager = new BufferedMemoryManager(dataPath);
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
      System.out.println("Passed BufferedMemoryManager int test");
    }
    //test int array, put should replace the current value
    int[] testarray = {234, 14123, 534, 6345};
    value = ByteBuffer.allocateDirect(16);
    ByteBuffer value2 = ByteBuffer.allocateDirect(16);
    ByteBuffer value3 = ByteBuffer.allocateDirect(16);
    ByteBuffer value4 = ByteBuffer.allocateDirect(16);

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
      System.out.println("Passed BufferedMemoryManager int array test");
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
      System.out.println("Passed BufferedMemoryManager int array iterator test,"
          + " number of values returned by"
          + "iterator : " + 1);
    }

    op.delete(key);
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
    key.putInt(1);
    for (int i : datamultiarray[0]) {
      value2.putInt(i);
    }
    op.put(key, value2);

    key.clear();
    key.putInt(3);
    for (int i : datamultiarray[2]) {
      value3.putInt(i);
    }
    op.put(key, value3);

    key.clear();
    key.putInt(2);
    for (int i : datamultiarray[1]) {
      value4.putInt(i);
    }
    op.put(key, value4);

    Iterator<Object> itermulti = op.iterator();
    int itercount = 0;
    itercount = 0;
    while (itermulti.hasNext()) {
      if (itercount > 3) {
        break;
      }
      dataset = (int[]) itermulti.next();
      for (int i = 0; i < 4; i++) {
        if (dataset[i] != datamultiarray[itercount][i]) {
          allPassed = false;
        }
      }
      itercount++;
    }

    if (allPassed) {
      System.out.println("Passed BufferedMemoryManager int multi array iterator test,"
          + " number of values returned by"
          + "iterator : " + itercount);
    }

    //test append function
    key.clear();
    value2.clear();
    key.putInt(6);
    for (int i : datamultiarray[0]) {
      value2.putInt(i);
    }
    op.put(key, value2);
    value3.clear();
    for (int i : datamultiarray[1]) {
      value3.putInt(i);
    }
    op.append(key, value3);

    results = op.get(key);
    for (int i : datamultiarray[0]) {
      if (i != results.getInt()) {
        allPassed = false;
      }
    }
    for (int i : datamultiarray[1]) {
      if (i != results.getInt()) {
        allPassed = false;
      }
    }

    if (allPassed) {
      System.out.println("Passed BufferedMemoryManager int array append test");
    }

    return allPassed;
  }
}

