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
package edu.iu.dsc.tws.data.memory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;

/**
 * Inserts into the memory store in batches. Only one instance per executor.
 */
public class BulkMemoryManager extends AbstractMemoryManager {

  private static final Logger LOG = Logger.getLogger(BulkMemoryManager.class.getName());

  /**
   * Memory manager implementaion
   */
  private MemoryManager memoryManager;

  /**
   * Keeps the limits and step sizes for each key that is added
   * the double array has 2 values 1st contains the limit and the second contains the step size
   */
  private Map<String, double[]> keyMap;

  /**
   * Keeps the current submitted count for a given key
   */
  private Map<String, Integer> keyMapCurrent;

  /**
   * Keeps the ByteBuffers that need to be written
   */
  private Map<String, LinkedList<ByteBuffer>> keyMapBuffers;

  public BulkMemoryManager(Path dataPath) {
    //TODO : This needs to be loaded from a configuration file
    //TODO: need to add Singleton pattern to make sure only one instance of MM is created per
    //executor
    memoryManager = new LMDBMemoryManager(dataPath);
    init();
  }


  @Override
  public boolean init() {
    keyMap = new ConcurrentHashMap<String, double[]>();
    keyMapCurrent = new ConcurrentHashMap<String, Integer>();
    keyMapBuffers = new ConcurrentHashMap<String, LinkedList<ByteBuffer>>();
    return false;
  }

  @Override
  public boolean append(ByteBuffer key, ByteBuffer value) {
    return memoryManager.append(key, value);
  }

  @Override
  public boolean append(long key, ByteBuffer value) {
    return memoryManager.append(key, value);
  }

  @Override
  public boolean put(ByteBuffer key, ByteBuffer value) {
    return memoryManager.put(key, value);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    return memoryManager.put(key, value);
  }

  @Override
  public boolean put(byte[] key, ByteBuffer value) {
    return memoryManager.put(key, value);
  }

  @Override
  public boolean put(long key, ByteBuffer value) {
    return memoryManager.put(key, value);
  }

  @Override
  public boolean put(long key, byte[] value) {
    return memoryManager.put(key, value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) {
    return memoryManager.get(key);
  }

  @Override
  public ByteBuffer get(byte[] key) {
    return memoryManager.get(key);
  }

  @Override
  public ByteBuffer get(long key) {
    return memoryManager.get(key);
  }

  @Override
  public byte[] getBytes(byte[] key) {
    return memoryManager.getBytes(key);
  }

  @Override
  public byte[] getBytes(long key) {
    return memoryManager.getBytes(key);
  }

  @Override
  public byte[] getBytes(ByteBuffer key) {
    return memoryManager.getBytes(key);
  }

  @Override
  public boolean containsKey(ByteBuffer key) {
    return memoryManager.containsKey(key);
  }

  @Override
  public boolean containsKey(byte[] key) {
    return memoryManager.containsKey(key);
  }

  @Override
  public boolean containsKey(long key) {
    return memoryManager.containsKey(key);
  }

  @Override
  public boolean delete(ByteBuffer key) {
    return memoryManager.delete(key);
  }

  @Override
  public boolean delete(byte[] key) {
    return memoryManager.delete(key);
  }

  @Override
  public boolean delete(long key) {
    return memoryManager.delete(key);
  }

  public Map<String, double[]> getKeyMap() {
    return keyMap;
  }

  public void setKeyMap(Map<String, double[]> keyMap) {
    this.keyMap = keyMap;
  }

  /**
   * Register the key
   *
   * @param key key value to be registered
   * @param limit the maximum number of values that will be submitted
   * @param step the step size. The Memory manager will write to the store once this value
   * is reached
   * @return true if the key was registered and false if the key is already present
   */
  public boolean registerKey(String key, int limit, int step) {
    //TODO : do we have knowledge of the size of each byteBuffer?
    if (keyMap.containsKey(key)) {
      return false;
    }
    double[] temp = {limit, step};
    keyMap.put(key, temp);
    keyMapCurrent.put(key, 0);
    keyMapBuffers.put(key, new LinkedList<ByteBuffer>());
    return true;
  }

  /**
   * Buffers the inputs before submitting it to the store
   */
  public boolean putBulk(String key, ByteBuffer value) {
    if (!keyMap.containsKey(key)) {
      LOG.info(String.format("No entry for the given key : %s .The key needs to"
          + " be registered with the BulkMemoryManager", key));
      return false;
    }
    //TODO: need to make sure that there are no memory leaks here
    //TODO: do we need to lock on key value? will more than 1 thread submit the same key
    double[] stats = keyMap.get(key);
    int currentCount = keyMapCurrent.get(key);
    // If this is the last value write all the values to store
    if (currentCount + 1 == stats[0]) {

      keyMap.remove(key);
      keyMapCurrent.remove(key);
      keyMapBuffers.remove(key);
    } else if ((currentCount + 1) % stats[1] == 0) {
      // write to store if the step has been met

      keyMapCurrent.put(key, currentCount + 1);
    } else {
      keyMapCurrent.put(key, currentCount + 1);
      keyMapBuffers.get(key).add(value);
    }
    //TODO : check if the return is correct just a place holder for now
    return true;
  }
}
