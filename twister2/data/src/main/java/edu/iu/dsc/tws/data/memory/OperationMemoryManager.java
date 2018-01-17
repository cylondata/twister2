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

/**
 * This controls the memory manager for a single operation. An example of an operation is a
 * gather, reduce.
 * Note: This class needs to implment all the methods in the Memory Manager interface without the
 * operationID parameter
 */
public class OperationMemoryManager {

  private int operationID;

  private MemoryManager parent;

  public OperationMemoryManager(int opID, MemoryManager parentMM) {
    this.operationID = opID;
    this.parent = parentMM;
    init();
  }

  public boolean init() {
    return true;
  }

  public boolean append(ByteBuffer key, ByteBuffer value) {
    return parent.append(operationID, key, value);
  }

  public boolean append(long key, ByteBuffer value) {
    return parent.append(operationID, key, value);
  }

  public boolean put(ByteBuffer key, ByteBuffer value) {
    return parent.put(operationID, key, value);
  }

  public boolean put(byte[] key, ByteBuffer value) {
    return parent.put(operationID, key, value);
  }

  public boolean put(long key, ByteBuffer value) {
    return parent.put(operationID, key, value);
  }

  public boolean put(byte[] key, byte[] value) {
    return parent.put(operationID, key, value);
  }

  public boolean put(long key, byte[] value) {
    return parent.put(operationID, key, value);
  }

  public ByteBuffer get(ByteBuffer key) {
    return parent.get(operationID, key);
  }

  public ByteBuffer get(byte[] key) {
    return parent.get(operationID, key);
  }

  public ByteBuffer get(long key) {
    return parent.get(operationID, key);
  }

  public byte[] getBytes(byte[] key) {
    return parent.getBytes(operationID, key);
  }

  public byte[] getBytes(long key) {
    return parent.getBytes(operationID, key);
  }

  public byte[] getBytes(ByteBuffer key) {
    return parent.getBytes(operationID, key);
  }

  public boolean containsKey(ByteBuffer key) {
    return parent.containsKey(operationID, key);
  }

  public boolean containsKey(byte[] key) {
    return parent.containsKey(operationID, key);
  }

  public boolean containsKey(long key) {
    return parent.containsKey(operationID, key);
  }

  public boolean delete(ByteBuffer key) {
    return parent.delete(operationID, key);
  }

  public boolean delete(byte[] key) {
    return parent.delete(operationID, key);
  }

  public boolean delete(long key) {
    return parent.delete(operationID, key);
  }

  public int getOperationID() {
    return operationID;
  }

  public void setOperationID(int operationID) {
    this.operationID = operationID;
  }

  public MemoryManager getParent() {
    return parent;
  }

  public void setParent(MemoryManager parent) {
    this.parent = parent;
  }
}
