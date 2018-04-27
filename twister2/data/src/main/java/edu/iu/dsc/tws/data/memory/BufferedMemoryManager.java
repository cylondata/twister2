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
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

/**
 * Inserts into the memory store in batches. Only one instance per executor.
 */
//TODO: need to address the issue of converting bytebuffers to string keys to improve performance
//TODO: one option would be to implement custom byteBuffer that is comparable
//TODO : Need to make the BMM work like a normal map. put should replace any exsisting value.
public class BufferedMemoryManager extends AbstractMemoryManager {

  private static final Logger LOG = Logger.getLogger(BufferedMemoryManager.class.getName());

  /**
   * Memory manager implementaion
   */
  private MemoryManager memoryManager;

  /**
   * Keeps the limits and step sizes for each key that is added
   * the double array has 2 values 1st contains the limit and the second contains the step size
   */
  private Map<Integer, Map<String, Integer>> keyMap;

  /**
   * Keeps the current submitted count for a given key
   */
  private Map<Integer, Map<String, Integer>> keyMapCurrent;

  /**
   * Keeps the ByteBuffers that need to be written
   */
  private Map<Integer, Map<String, LinkedList<ByteBuffer>>> keyMapBuffers;

  /**
   * Keeps track of the collective size of byte buffers for each key that is currently held
   * in the keyMapBuffers
   */
  private Map<Integer, Map<String, Integer>> keyBufferSizes;

  public BufferedMemoryManager(Path dataPath) {
    //TODO : This needs to be loaded from a configuration file
    //TODO: need to add Singleton pattern to make sure only one instance of MM is created per
    //executor
    memoryManager = new LMDBMemoryManager(dataPath);
    init();
  }


  @Override
  public boolean init() {

    keyMap = new HashMap<Integer, Map<String, Integer>>();
    keyMapCurrent = new HashMap<Integer, Map<String, Integer>>();
    keyMapBuffers = new HashMap<Integer, Map<String, LinkedList<ByteBuffer>>>();
    keyBufferSizes = new HashMap<Integer, Map<String, Integer>>();
    operationMap = new HashMap<Integer, OperationMemoryManager>();
    return false;
  }

  @Override
  public boolean append(int opID, ByteBuffer key, ByteBuffer value) {
    if (key.position() != 0) {
      key.flip();
    }
    String keyString = MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString();
    return appendBulk(opID, keyString, value);
  }

  /*@Override
  public boolean append(int opID, byte[] key, ByteBuffer value) {
    return false;
  }

  @Override
  public boolean append(int opID, long key, ByteBuffer value) {
    return memoryManager.append(opID, key, value);
  }*/

  @Override
  public boolean append(int opID, String key, ByteBuffer value) {
    return appendBulk(opID, key, value);
  }

  /*@Override
  public <T extends Serializable> boolean append(int opID, T key, ByteBuffer value) {
    return false;
  }*/

  @Override
  public boolean put(int opID, ByteBuffer key, ByteBuffer value) {
    if (key.position() != 0) {
      key.flip();
    }
    String keyString = MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString();
    return putBulk(opID, keyString, value);
  }

  @Override
  public boolean put(int opID, byte[] key, byte[] data) {
    return false;
  }

  /*@Override
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
  }*/

  @Override
  public boolean put(int opID, String key, ByteBuffer value) {
    return putBulk(opID, key, value);
  }

  /*@Override
  public <T extends Serializable> boolean put(int opID, T key, ByteBuffer value) {
    return false;
  }

  @Override
  public boolean put(int opID, long key, byte[] value) {
    return memoryManager.put(opID, key, value);
  }*/

  /*@Override
  public boolean put(int opID, String key, byte[] value) {
    return false;
  }

  @Override
  public <T extends Serializable> boolean put(int opID, T key, byte[] value) {
    return false;
  }*/

  @Override
  public ByteBuffer get(int opID, ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }
    String keyString = MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString();
    if (keyMap.get(opID).containsKey(keyString)) {
      flush(opID, keyString);
    }
    return memoryManager.get(opID, key);
  }

  /*@Override
  public ByteBuffer get(int opID, byte[] key) {
    return memoryManager.get(opID, key);
  }

  @Override
  public ByteBuffer get(int opID, long key) {
    return memoryManager.get(opID, key);
  }*/

  public ByteBuffer get(int opID, String key) {
    // if the key given is already in the keyMap we need to flush the key
    //TODO: Do we flush the key and get the data from the memory store or do we get what
    //TODO: we can from the keymap and then get the rest of the store
    if (keyMap.get(opID).containsKey(key)) {
      flush(opID, key);
    }

    return memoryManager.get(opID, key);
  }

  /*@Override
  public <T extends Serializable> ByteBuffer get(int opID, T key) {
    return null;
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
  public byte[] getBytes(int opID, String key) {
    return new byte[0];
  }

  @Override
  public <T extends Serializable> byte[] getBytes(int opID, T key) {
    return new byte[0];
  }

  @Override
  public byte[] getBytes(int opID, ByteBuffer key) {
    return memoryManager.getBytes(opID, key);
  }*/

  @Override
  public boolean containsKey(int opID, ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }
    return containsKey(opID, MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString());
  }

  /*@Override
  public boolean containsKey(int opID, byte[] key) {
    return memoryManager.containsKey(opID, key);
  }

  @Override
  public boolean containsKey(int opID, long key) {
    return memoryManager.containsKey(opID, key);
  }*/

  @Override
  public boolean containsKey(int opID, String key) {
    if (keyMap.get(opID).containsKey(key)) {
      return true;
    }
    return memoryManager.containsKey(opID, key);
  }

  /*@Override
  public <T extends Serializable> boolean containsKey(int opID, T key) {
    return false;
  }*/

  @Override
  public boolean delete(int opID, ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }
    deleteFromBMM(opID, MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString());
    return memoryManager.delete(opID, key);
  }

  /*@Override
  public boolean delete(int opID, byte[] key) {
    deleteFromBMM(opID, new String(key, java.nio.charset.StandardCharsets.UTF_8));
    return memoryManager.delete(opID, key);
  }

  @Override
  public boolean delete(int opID, long key) {
    deleteFromBMM(opID, new String(Longs.toByteArray(key),java.nio.charset.StandardCharsets.UTF_8));
    return memoryManager.delete(opID, key);
  }*/

  @Override
  public boolean delete(int opID, String key) {
    deleteFromBMM(opID, key);
    return memoryManager.delete(opID, key);
  }

  /*@Override
  public <T extends Serializable> boolean delete(int opID, T key) {
    //TODO : Need to think about the 511 key size constraint in lmdb, need to check that for other
    //scenarios as well
    return memoryManager.delete(opID, key);
  }*/

  public void deleteFromBMM(int opID, String key) {
    keyMap.get(opID).remove(key);
    keyMapCurrent.get(opID).remove(key);
    keyMapBuffers.get(opID).remove(key);
    keyBufferSizes.get(opID).remove(key);
  }

  @Override
  public OperationMemoryManager addOperation(int opID, DataMessageType type) {
    if (operationMap.containsKey(opID)) {
      return null;
    }
    OperationMemoryManager temp = new OperationMemoryManager(opID, type, this);
    memoryManager.addOperation(opID, type);
    keyMap.put(opID, new ConcurrentHashMap<String, Integer>());
    keyMapCurrent.put(opID, new ConcurrentHashMap<String, Integer>());
    keyMapBuffers.put(opID, new ConcurrentHashMap<String, LinkedList<ByteBuffer>>());
    keyBufferSizes.put(opID, new ConcurrentHashMap<String, Integer>());
    operationMap.put(opID, temp);
    return temp;
  }

  @Override
  public OperationMemoryManager addOperation(int opID, DataMessageType type,
                                             DataMessageType keyType) {
    if (operationMap.containsKey(opID)) {
      return null;
    }
    OperationMemoryManager temp = new OperationMemoryManager(opID, type, keyType, this);
    memoryManager.addOperation(opID, keyType, type);
    keyMap.put(opID, new ConcurrentHashMap<String, Integer>());
    keyMapCurrent.put(opID, new ConcurrentHashMap<String, Integer>());
    keyMapBuffers.put(opID, new ConcurrentHashMap<String, LinkedList<ByteBuffer>>());
    keyBufferSizes.put(opID, new ConcurrentHashMap<String, Integer>());
    operationMap.put(opID, temp);
    return temp;
  }

  @Override
  public boolean removeOperation(int opID) {
    memoryManager.removeOperation(opID);
    keyMap.remove(opID);
    keyMapCurrent.remove(opID);
    keyMapBuffers.remove(opID);
    keyBufferSizes.remove(opID);
    operationMap.remove(opID);
    return true;
  }

  /**
   * Register the key
   *
   * @param key key value to be registered
   * @param step the step size. The Memory manager will write to the store once this value
   * is reached
   * @return true if the key was registered and false if the key is already present
   */
  public boolean registerKey(int opID, String key, int step) {
    //TODO : do we have knowledge of the size of each byteBuffer?
    if (keyMap.get(opID).containsKey(key)) {
      return false;
    }
    keyMap.get(opID).put(key, step);
    keyMapCurrent.get(opID).put(key, 0);
    keyMapBuffers.get(opID).put(key, new LinkedList<ByteBuffer>());
    keyBufferSizes.get(opID).put(key, 0);
    return true;
  }

  public boolean registerKey(int opID, String key) {
    return registerKey(opID, key, MemoryManagerContext.BULK_MM_STEP_SIZE);
  }

  /**
   * Buffers the inputs before submitting it to the store. If the value is already present it
   * will be replaced
   */
  public boolean putBulk(int opID, String key, ByteBuffer value) {
    if (value.position() != 0) {
      value.flip();
    }
    if (!keyMap.get(opID).containsKey(key) && !memoryManager.containsKey(opID, key)) {
      registerKey(opID, key, MemoryManagerContext.BULK_MM_STEP_SIZE);
    } else {
      //If the key is already present we need to replace its value so we need to clear the data
      delete(opID, key);
      registerKey(opID, key, MemoryManagerContext.BULK_MM_STEP_SIZE);
    }

    keyMapCurrent.get(opID).put(key, 1);
    keyMapBuffers.get(opID).get(key).add(value);
    keyBufferSizes.get(opID).put(key, keyBufferSizes.get(opID).get(key) + value.limit());
    return true;
  }

  /**
   * Buffers the inputs before submitting to the store. The new values will be appended to the end
   */
  public boolean appendBulk(int opID, String key, ByteBuffer value) {
    if (value.position() != 0) {
      value.flip();
    }
    if (!keyMap.get(opID).containsKey(key)) {
      registerKey(opID, key, MemoryManagerContext.BULK_MM_STEP_SIZE);
    }
    //TODO: need to make sure that there are no memory leaks here
    //TODO: do we need to lock on key value? will more than 1 thread submit the same key
    int step = keyMap.get(opID).get(key);
    int currentCount = keyMapCurrent.get(opID).get(key);
    // If this is the last value write all the values to store
    if ((currentCount + 1) % step == 0) {
      // write to store if the step has been met
      flush(opID, key, value);
      keyMapCurrent.get(opID).put(key, currentCount + 1);
    } else {
      keyMapCurrent.get(opID).put(key, currentCount + 1);
      keyMapBuffers.get(opID).get(key).add(value);
      keyBufferSizes.get(opID).put(key, keyBufferSizes.get(opID).get(key) + value.limit());
    }
    //TODO : check if the return is correct just a place holder for now
    return true;
  }

  @Override
  public boolean flush(int opID, ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }
    return flush(opID, MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString());
  }

  /*@Override
  public boolean flush(int opID, byte[] key) {
    return flush(opID, new String(key, java.nio.charset.StandardCharsets.UTF_8));
  }

  @Override
  public boolean flush(int opID, long key) {
    return flush(opID, new String(Longs.toByteArray(key), java.nio.charset.StandardCharsets.UTF_8));
  }*/

  /**
   * Makes sure all the data that is held in the BufferedMemoryManager is pushed into the
   * memory store
   */
  public boolean flush(int opID, String key) {
    ByteBuffer temp = ByteBuffer.allocateDirect(keyBufferSizes.get(opID).get(key));
    LinkedList<ByteBuffer> buffers = keyMapBuffers.get(opID).get(key);
    while (!buffers.isEmpty()) {
      temp.put(buffers.poll());
    }
    //Since we got all the buffer values reset the size
    keyMap.get(opID).remove(key);
    keyMapCurrent.get(opID).remove(key);
    keyMapBuffers.get(opID).remove(key);
    keyBufferSizes.get(opID).remove(key);
    if (memoryManager.containsKey(opID, key)) {
      return memoryManager.append(opID, key,
          temp);
    } else {
      return memoryManager.put(opID, key,
          temp);
    }
  }

  @Override
  public boolean flush(int opID) {
    return false;
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
    ByteBuffer temp = ByteBuffer.allocateDirect(keyBufferSizes.get(opID).get(key) + last.limit());
    LinkedList<ByteBuffer> buffers = keyMapBuffers.get(opID).get(key);
    while (!buffers.isEmpty()) {
      temp.put(buffers.poll());
    }
    temp.put(last);
    //Since we got all the buffer values reset the size
    keyMap.get(opID).remove(key);
    keyMapCurrent.get(opID).remove(key);
    keyMapBuffers.get(opID).remove(key);
    keyBufferSizes.get(opID).remove(key);
    if (memoryManager.containsKey(opID, key)) {
      return memoryManager.append(opID, key,
          temp);
    } else {
      return memoryManager.put(opID, key,
          temp);
    }
  }

  /**
   * Flush all the keys that are stored for the given operation id.
   * this must be called before a range of keys will be read as in the iterator methods
   */
  public boolean flushAll(int opID) {
    for (String s : keyMap.get(opID).keySet()) {
      flush(opID, s);
    }
    return true;
  }
  /*@Override
  public <T extends Serializable> boolean flush(int opID, T key) {
    return false;
  }*/

  @Override
  public boolean close(int opID, ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }
    return close(opID, MemoryManagerContext.DEFAULT_CHARSET.decode(key).toString());
  }

  /*@Override
  public boolean close(int opID, byte[] key) {
    return close(opID, new String(key, java.nio.charset.StandardCharsets.UTF_8));
  }

  @Override
  public boolean close(int opID, long key) {
    return close(opID, new String(Longs.toByteArray(key), java.nio.charset.StandardCharsets.UTF_8));
  }*/

  /**
   * Closing the key will make the BufferedMemoryManager to flush the current data into the store
   * and delete all the key information. This is done once we know that no more values will be sent
   * for this key
   */
  @Override
  public boolean close(int opID, String key) {
    flush(opID, key);
    keyMap.get(opID).remove(key);
    keyMapCurrent.get(opID).remove(key);
    keyMapBuffers.get(opID).remove(key);
    keyBufferSizes.get(opID).remove(key);
    return true;
  }

  @Override
  public Iterator<Object> getIterator(int opID, DataMessageType keyType, DataMessageType valueType,
                                      KryoMemorySerializer deSerializer, ByteOrder order) {
    flushAll(opID);
    return memoryManager.getIterator(opID, keyType, valueType, deSerializer, order);
  }

  @Override
  public Iterator<Object> getIterator(int opID, DataMessageType valueType,
                                      KryoMemorySerializer deSerializer, ByteOrder order) {
    flushAll(opID);
    return memoryManager.getIterator(opID, valueType, deSerializer, order);
  }

  /*@Override
  public <T extends Serializable> boolean close(int opID, T key) {
    return false;
  }*/
}
