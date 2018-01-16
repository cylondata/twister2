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
import java.util.HashMap;
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
  private Map<String, Integer> keyMap;

  /**
   * Keeps the current submitted count for a given key
   */
  private Map<String, Integer> keyMapCurrent;

  /**
   * Keeps the ByteBuffers that need to be written
   */
  private Map<String, LinkedList<ByteBuffer>> keyMapBuffers;

  /**
   * Keeps track of the collective size of byte buffers for each key that is currently held
   * in the keyMapBuffers
   */
  private Map<String, Integer> keyBufferSizes;

  public BulkMemoryManager(Path dataPath) {
    //TODO : This needs to be loaded from a configuration file
    //TODO: need to add Singleton pattern to make sure only one instance of MM is created per
    //executor
    memoryManager = new LMDBMemoryManager(dataPath);
    init();
  }


  @Override
  public boolean init() {
    keyMap = new ConcurrentHashMap<String, Integer>();
    keyMapCurrent = new ConcurrentHashMap<String, Integer>();
    keyMapBuffers = new ConcurrentHashMap<String, LinkedList<ByteBuffer>>();
    operationMap = new HashMap<Integer, OperationMemoryManager>();
    return false;
  }

  @Override
  public boolean append(int opID, ByteBuffer key, ByteBuffer value) {
    return memoryManager.append(opID, key, value);
  }

  @Override
  public boolean append(int opID, long key, ByteBuffer value) {
    return memoryManager.append(opID, key, value);
  }

  @Override
  public boolean put(int opID, ByteBuffer key, ByteBuffer value) {
    return memoryManager.put(opID, key, value);
  }

  @Override
  public boolean put(int opID, byte[] key, byte[] value) {
    return memoryManager.put(opID, key, value);
  }

  @Override
  public boolean put(int opID, byte[] key, ByteBuffer value) {
    return memoryManager.put(opID, key, value);
  }

  @Override
  public boolean put(int opID, long key, ByteBuffer value) {
    return memoryManager.put(opID, key, value);
  }

  @Override
  public boolean put(int opID, long key, byte[] value) {
    return memoryManager.put(opID, key, value);
  }

  @Override
  public ByteBuffer get(int opID, ByteBuffer key) {
    return memoryManager.get(opID, key);
  }

  @Override
  public ByteBuffer get(int opID, byte[] key) {
    return memoryManager.get(opID, key);
  }

  @Override
  public ByteBuffer get(int opID, long key) {
    return memoryManager.get(opID, key);
  }

  public ByteBuffer get(int opID, String key) {
    // if the key given is already in the keyMap we need to flush the key
    //TODO: Do we flush the key and get the data from the memory store or do we get what
    //TODO: we can from the keymap and then get the rest of the store
    if (keyMap.containsKey(key)) {
      flush(opID, key);
    }

    return memoryManager.get(opID, ByteBuffer.wrap(key.getBytes(MemoryManagerContext.DEFAULT_CHARSET)));
  }

  @Override
  public byte[] getBytes(int opID, byte[] key) {
    return memoryManager.getBytes(opID, key);
  }

  @Override
  public byte[] getBytes(int opID, long key) {
    return memoryManager.getBytes(opID, key);
  }

  @Override
  public byte[] getBytes(int opID, ByteBuffer key) {
    return memoryManager.getBytes(opID, key);
  }

  @Override
  public boolean containsKey(int opID, ByteBuffer key) {
    return memoryManager.containsKey(opID, key);
  }

  @Override
  public boolean containsKey(int opID, byte[] key) {
    return memoryManager.containsKey(opID, key);
  }

  @Override
  public boolean containsKey(int opID, long key) {
    return memoryManager.containsKey(opID, key);
  }

  @Override
  public boolean delete(int opID, ByteBuffer key) {
    return memoryManager.delete(opID, key);
  }

  @Override
  public boolean delete(int opID, byte[] key) {
    return memoryManager.delete(opID, key);
  }

  @Override
  public boolean delete(int opID, long key) {
    return memoryManager.delete(opID, key);
  }

  @Override
  public OperationMemoryManager addOperation(int opID) {
    if(operationMap.containsKey(opID)){
      return null;
    }
    OperationMemoryManager temp = new OperationMemoryManager(opID, this);
    memoryManager.addOperation(opID);
    operationMap.put(opID, temp);
    return temp;
  }

  public Map<String, Integer> getKeyMap() {
    return keyMap;
  }

  public void setKeyMap(Map<String, Integer> keyMap) {
    this.keyMap = keyMap;
  }

  /**
   * Register the key
   *
   * @param key key value to be registered
   * @param step the step size. The Memory manager will write to the store once this value
   * is reached
   * @return true if the key was registered and false if the key is already present
   */
  public boolean registerKey(String key, int step) {
    //TODO : do we have knowledge of the size of each byteBuffer?
    if (keyMap.containsKey(key)) {
      return false;
    }
    keyMap.put(key, step);
    keyMapCurrent.put(key, 0);
    keyMapBuffers.put(key, new LinkedList<ByteBuffer>());
    return true;
  }

  public boolean registerKey(String key) {
    return registerKey(key, MemoryManagerContext.BULK_MM_STEP_SIZE);
  }

  /**
   * Buffers the inputs before submitting it to the store
   */
  public boolean putBulk(int opID, String key, ByteBuffer value) {
    if (!keyMap.containsKey(key)) {
      LOG.info(String.format("No entry for the given key : %s .The key needs to"
          + " be registered with the BulkMemoryManager", key));
      return false;
    }
    //TODO: need to make sure that there are no memory leaks here
    //TODO: do we need to lock on key value? will more than 1 thread submit the same key
    int step = keyMap.get(key);
    int currentCount = keyMapCurrent.get(key);
    // If this is the last value write all the values to store
    if ((currentCount + 1) % step == 0) {
      // write to store if the step has been met
      flush(opID, key, value);
      keyMapCurrent.put(key, currentCount + 1);
    } else {
      keyMapCurrent.put(key, currentCount + 1);
      keyMapBuffers.get(key).add(value);
      keyBufferSizes.put(key, keyBufferSizes.get(key) + value.limit());
    }
    //TODO : check if the return is correct just a place holder for now
    return true;
  }

  /**
   * Makes sure all the data that is held in the BulkMemoryManager is pushed into the
   * memory store
   */
  public boolean flush(int opID, String key) {
    ByteBuffer temp = ByteBuffer.allocateDirect(keyBufferSizes.get(key));
    LinkedList<ByteBuffer> buffers = keyMapBuffers.get(key);
    while (!buffers.isEmpty()) {
      temp.put(buffers.poll());
    }
    //Since we got all the buffer values reset the size
    keyBufferSizes.put(key, 0);
    return memoryManager.put(opID, ByteBuffer.wrap(key.getBytes(MemoryManagerContext.DEFAULT_CHARSET)),
        temp);
  }

  /**
   * Slight variation of flush for so that the last ByteBuffer does not need to be copied into the
   * map
   *
   * @param key key to flush
   * @param last the last value that needs to be appended to the ByteBuffers that correspond to the
   * given key
   */
  public boolean flush(int opID, String key, ByteBuffer last) {
    ByteBuffer temp = ByteBuffer.allocateDirect(keyBufferSizes.get(key) + last.limit());
    LinkedList<ByteBuffer> buffers = keyMapBuffers.get(key);
    while (!buffers.isEmpty()) {
      temp.put(buffers.poll());
    }
    temp.put(last);
    //Since we got all the buffer values reset the size
    keyBufferSizes.put(key, 0);
    return memoryManager.put(opID, ByteBuffer.wrap(key.getBytes(MemoryManagerContext.DEFAULT_CHARSET)),
        temp);
  }

  /**
   * Closing the key will make the BulkMemoryManager to flush the current data into the store and
   * delete all the key information. This is done once we know that no more values will be sent for
   * this key
   */
  public boolean close(int opID, String key) {
    flush(opID, key);
    keyMap.remove(key);
    keyMapCurrent.remove(key);
    keyMapBuffers.remove(key);
    return true;
  }
}
