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

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;

/**
 * Created by pulasthi on 1/4/18.
 */
public class BulkMemoryManager extends AbstractMemoryManager {

  /**
   * Memory manager implementaion
   */
  private MemoryManager memoryManager;

  public BulkMemoryManager(Path dataPath) {
    //TODO : This needs to be loaded from a configuration file
    memoryManager = new LMDBMemoryManager(dataPath);
    init();
  }

  @Override
  public boolean init() {
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
}
