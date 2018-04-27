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
import java.util.Iterator;

import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

/**
 * base interface for memory managers. Memory managers are responsible of keeping data in memoory
 * for various requirements. The memory manager is also responsible of writing data to disk when
 * the memory does not have enough space
 */
public interface MemoryManager {

  /**
   * Initializes the Memory manager
   */
  boolean init();

  /**
   * Appends the given byte buffer to the current value in the store.
   * If the store does not contain the given key append works as a put operation
   *
   * @param opID id value of the operation
   * @param key key to append to
   * @param value value to be appended
   * @return true if append was performed, false otherwise
   */
  boolean append(int opID, ByteBuffer key, ByteBuffer value);

  /*boolean append(int opID, byte[] key, ByteBuffer value);

  boolean append(int opID, long key, ByteBuffer value);*/

  boolean append(int opID, String key, ByteBuffer value);

  /*<T extends Serializable> boolean append(int opID, T key, ByteBuffer value);*/

  /**
   * Stores the give key value pair in the memory manager
   */
  boolean put(int opID, ByteBuffer key, ByteBuffer value);

  boolean put(int opID, byte[] key, byte[] data);
  /**
   * Stores the give key value pair in the memory manager
   * if the key already exists the new value will be added to the key
   */
  /*boolean put(int opID, byte[] key, ByteBuffer value);

  */

  /**
   * Stores the give key value pair in the memory manager
   *//*
  boolean put(int opID, long key, ByteBuffer value);*/

  boolean put(int opID, String key, ByteBuffer value);

  /*<T extends Serializable> boolean put(int opID, T key, ByteBuffer value);*/

  /**
   * Stores the give key value pair in the memory manager
   *
   * @param key key of the pair
   * @param value value to be stored
   * @return true of the key value pair was added, false otherwise
   */
  /*boolean put(int opID, byte[] key, byte[] value);

  *//**
   * Stores the give key value pair in the memory manager
   *//*
  boolean put(int opID, long key, byte[] value);

  boolean put(int opID, String key, byte[] value);

  <T extends Serializable> boolean put(int opID, T key, byte[] value);*/

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   */
  ByteBuffer get(int opID, ByteBuffer key);

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   */
  /*ByteBuffer get(int opID, byte[] key);

  */

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   *//*
  ByteBuffer get(int opID, long key);*/

  ByteBuffer get(int opID, String key);

  /*<T extends Serializable> ByteBuffer get(int opID, T key);

  byte[] getBytes(int opID, ByteBuffer key);

  *//**
   * Get the corresponding value as bytes for the given key from the store
   *//*
  byte[] getBytes(int opID, byte[] key);

  *//**
   * Get the corresponding value as bytes for the given key from the store
   *//*
  byte[] getBytes(int opID, long key);

  *//**
   * Get the corresponding value as bytes for the given key from the store
   *//*

  byte[] getBytes(int opID, String key);

  <T extends Serializable> byte[] getBytes(int opID, T key);*/


  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   */
  boolean containsKey(int opID, ByteBuffer key);

  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   */
  /*boolean containsKey(int opID, byte[] key);

  */

  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   *//*
  boolean containsKey(int opID, long key);*/

  boolean containsKey(int opID, String key);

  /*<T extends Serializable> boolean containsKey(int opID, T key);*/

  /**
   * delete the given key from the store
   */
  boolean delete(int opID, ByteBuffer key);

  /**
   * delete the given key from the store
   *//*
  boolean delete(int opID, byte[] key);

  */

  /**
   * delete the given key from the store
   *//*
  boolean delete(int opID, long key);*/

  boolean delete(int opID, String key);

  /*<T extends Serializable> boolean delete(int opID, T key);*/

  /**
   * Add an memory manager instance for the given operation
   *
   * @return returns the memory manager or null (Null is returned at the base layer)
   */
  OperationMemoryManager addOperation(int opID, DataMessageType type);

  OperationMemoryManager addOperation(int opID, DataMessageType messageType,
                                      DataMessageType keyType);

  /**
   * Remove an operation from the memory manager. This will result in loss of all the data related
   * to the operation in the memory manager
   *
   * @param opID operation id to be removed
   * @return true if the operation was removed, false otherwise
   */
  boolean removeOperation(int opID);

  /**
   * Flush all the data into the store. This method is needed only if buffering is done at the
   * Memory Manager
   *
   * @param opID id of the operation
   * @param key key to be flushed
   */
  boolean flush(int opID, ByteBuffer key);

  /*boolean flush(int opID, byte[] key);

  boolean flush(int opID, long key);*/

  boolean flush(int opID, String key);

  boolean flush(int opID);
  /*<T extends Serializable> boolean flush(int opID, T key);*/

  /**
   * flush and close the given key. After close is performed the key data will only be available in
   * the store. This method is needed only if buffering is done at the Memory Manager
   *
   * @param opID id of the operation
   * @param key key to be closed
   */
  boolean close(int opID, ByteBuffer key);

  /*boolean close(int opID, byte[] key);

  boolean close(int opID, long key);*/

  boolean close(int opID, String key);

  /*<T extends Serializable> boolean close(int opID, T key);*/

  Iterator<Object> getIterator(int opID, DataMessageType keyType, DataMessageType valueType,
                               KryoMemorySerializer deSerializer, ByteOrder order);

  Iterator<Object> getIterator(int opID, DataMessageType valueType,
                               KryoMemorySerializer deSerializer, ByteOrder order);

}
