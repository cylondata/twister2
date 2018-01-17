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
import java.util.Map;

import static java.nio.ByteBuffer.allocateDirect;

/**
 * Abstract Memory Manager. This class contains some of the methods that need to be shared between
 * all the memory manager implementations
 */
public abstract class AbstractMemoryManager implements MemoryManager {


  /**
   * Map that keeps all the OperationMemoryManager's
   */
  protected Map<Integer, OperationMemoryManager> operationMap;

  protected AbstractMemoryManager() {
    operationMap = new HashMap<Integer, OperationMemoryManager>();
  }

  public Object getDeserialized(ByteBuffer key) {
    return null;
  }

  public Object getDeserialized(byte[] key) {
    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return getDeserialized(keyBuffer);
  }

  public Object getDeserialized(long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return getDeserialized(keyBuffer);
  }
}
