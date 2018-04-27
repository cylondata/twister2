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

import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

/**
 * This controls the memory manager for a single operation. An example of an operation is a
 * gather, reduce.
 * Note: This class needs to implment all the methods in the Memory Manager interface without the
 * operationID parameter
 * <p>
 * TODO: We can latter look into making the operation manager typed. Based on the messageType
 * TODO: Then the get methods such as the iterator can directly return the typed objects
 */
public class OperationMemoryManager {

  private int operationID;

  private MemoryManager memoryManager;

  /**
   * is true if the associated operation is keyed
   */
  private boolean isKeyed;

  private DataMessageType keyType;

  private DataMessageType messageType;

  private KryoMemorySerializer deSerializer;

  private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

  public OperationMemoryManager(int opID, DataMessageType type, MemoryManager parentMM) {
    this.operationID = opID;
    this.memoryManager = parentMM;
    this.messageType = type;
    this.deSerializer = new KryoMemorySerializer();
    deSerializer.init(new HashMap<String, Object>());
    isKeyed = false;
    init();
  }

  public OperationMemoryManager(int opID, DataMessageType type, DataMessageType keyType,
                                MemoryManager parentMM) {
    this.operationID = opID;
    this.memoryManager = parentMM;
    this.messageType = type;
    this.keyType = keyType;
    this.deSerializer = new KryoMemorySerializer();
    deSerializer.init(new HashMap<String, Object>());
    isKeyed = true;
    init();
  }

  public boolean init() {
    return true;
  }

  public boolean append(ByteBuffer key, ByteBuffer value) {
    return memoryManager.append(operationID, key, value);
  }

  /*public boolean append(byte[] key, ByteBuffer value) {
    return memoryManager.append(operationID, key, value);
  }

  public boolean append(long key, ByteBuffer value) {
    return memoryManager.append(operationID, key, value);
  }*/

  public boolean append(String key, ByteBuffer value) {
    return memoryManager.append(operationID, key, value);
  }

  /*public <T extends Serializable> boolean append(T key, ByteBuffer value) {
    return memoryManager.append(operationID, key, value);
  }*/

  public boolean put(ByteBuffer key, ByteBuffer value) {
    return memoryManager.put(operationID, key, value);
  }

  public boolean put(byte[] key, byte[] data) {
    return memoryManager.put(operationID, key, data);
  }
  /*public boolean put(byte[] key, ByteBuffer value) {
    return memoryManager.put(operationID, key, value);
  }

  public boolean put(long key, ByteBuffer value) {
    return memoryManager.put(operationID, key, value);
  }*/

  public boolean put(String key, ByteBuffer value) {
    return memoryManager.put(operationID, key, value);
  }

  /*public <T extends Serializable> boolean put(T key, ByteBuffer value) {
    return memoryManager.put(operationID, key, value);
  }

  public boolean put(byte[] key, byte[] value) {
    return memoryManager.put(operationID, key, value);
  }

  public boolean put(long key, byte[] value) {
    return memoryManager.put(operationID, key, value);
  }

  public boolean put(String key, byte[] value) {
    return memoryManager.put(operationID, key, value);
  }

  public <T extends Serializable> boolean put(T key, byte[] value) {
    return memoryManager.put(operationID, key, value);
  }*/

  public ByteBuffer get(ByteBuffer key) {
    return memoryManager.get(operationID, key);
  }

  /*public ByteBuffer get(byte[] key) {
    return memoryManager.get(operationID, key);
  }

  public ByteBuffer get(long key) {
    return memoryManager.get(operationID, key);
  }*/

  public ByteBuffer get(String key) {
    return memoryManager.get(operationID, key);
  }

 /* public <T extends Serializable> ByteBuffer get(T key) {
    return memoryManager.get(operationID, key);
  }

  public byte[] getBytes(ByteBuffer key) {
    return memoryManager.getBytes(operationID, key);
  }

  public byte[] getBytes(byte[] key) {
    return memoryManager.getBytes(operationID, key);
  }

  public byte[] getBytes(long key) {
    return memoryManager.getBytes(operationID, key);
  }

  public byte[] getBytes(String key) {
    return memoryManager.getBytes(operationID, key);
  }

  public <T extends Serializable> byte[] getBytes(T key) {
    return memoryManager.getBytes(operationID, key);
  }*/

  public boolean containsKey(ByteBuffer key) {
    return memoryManager.containsKey(operationID, key);
  }

  /*public boolean containsKey(byte[] key) {
    return memoryManager.containsKey(operationID, key);
  }

  public boolean containsKey(long key) {
    return memoryManager.containsKey(operationID, key);
  }*/

  public boolean containsKey(String key) {
    return memoryManager.containsKey(operationID, key);
  }

  /*public <T extends Serializable> boolean containsKey(T key) {
    return memoryManager.containsKey(operationID, key);
  }*/

  public boolean delete(ByteBuffer key) {
    return memoryManager.delete(operationID, key);
  }

  /*public boolean delete(byte[] key) {
    return memoryManager.delete(operationID, key);
  }

  public boolean delete(long key) {
    return memoryManager.delete(operationID, key);
  }*/

  public boolean delete(String key) {
    return memoryManager.delete(operationID, key);
  }

  /*public <T extends Serializable> boolean delete(T key) {
    return memoryManager.delete(operationID, key);
  }*/

  public boolean flush(ByteBuffer key) {
    return memoryManager.flush(operationID, key);
  }

  public boolean flush() {
    return memoryManager.flush(operationID);
  }

  /*public boolean flush(byte[] key) {
    return memoryManager.flush(operationID, key);
  }

  public boolean flush(long key) {
    return memoryManager.flush(operationID, key);
  }*/

  public boolean flush(String key) {
    return memoryManager.flush(operationID, key);
  }

  /*public <T extends Serializable> boolean flush(int opID, T key) {
    return memoryManager.flush(operationID, key);
  }*/

  public boolean close(ByteBuffer key) {
    return memoryManager.close(operationID, key);
  }

  /*public boolean close(byte[] key) {
    return memoryManager.close(operationID, key);
  }

  public boolean close(long key) {
    return memoryManager.close(operationID, key);
  }*/

  public boolean close(String key) {
    return memoryManager.close(operationID, key);
  }

  public int getOperationID() {
    return operationID;
  }

  public void setOperationID(int operationID) {
    this.operationID = operationID;
  }

  public MemoryManager getMemoryManager() {
    return memoryManager;
  }

  public void setMemoryManager(MemoryManager memoryManager) {
    this.memoryManager = memoryManager;
  }

  /**
   * returns the deserialized data as a iterator
   * if the operation is keyed the iterator will return a list of pairs
   * if the operation is not keyed it will return a list of objects
   */
  public Iterator<Object> iterator() {
    if (isKeyed) {
      return memoryManager.getIterator(operationID, keyType, messageType, deSerializer, byteOrder);
    } else {
      return memoryManager.getIterator(operationID, messageType, deSerializer, byteOrder);
    }
  }

  public Iterator<Object> iterator(ByteOrder order) {
    if (isKeyed) {
      return memoryManager.getIterator(operationID, keyType, messageType, deSerializer, order);
    } else {
      return memoryManager.getIterator(operationID, messageType, deSerializer, order);
    }
  }

  public DataMessageType getKeyType() {
    return keyType;
  }

  public void setKeyType(DataMessageType keyType) {
    this.keyType = keyType;
  }

  public DataMessageType getMessageType() {
    return messageType;
  }

  public void setMessageType(DataMessageType messageType) {
    this.messageType = messageType;
  }

  public boolean isKeyed() {
    return isKeyed;
  }

  public void setKeyed(boolean keyed) {
    isKeyed = keyed;
  }
}
