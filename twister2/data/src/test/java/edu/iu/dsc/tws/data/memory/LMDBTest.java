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

import com.google.common.primitives.Ints;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;

/**
 * Created by pulasthi on 1/2/18.
 */
public class LMDBTest {
  public static void main(String[] args) {
    //Memory Manager
    LMDBTest test = new LMDBTest();
    test.testPrimitives();
  }

  public boolean testPrimitives() {
    boolean allPassed = true;
    Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase");
    MemoryManager memoryManager = new LMDBMemoryManager(dataPath);
    int opID = 1;
    memoryManager.addOperation(opID, DataMessageType.INTEGER);

    //Test single integer operation
    ByteBuffer key = ByteBuffer.allocateDirect(4);
    ByteBuffer value = ByteBuffer.allocateDirect(4);
    key.putInt(1);
    int testInt = 1231212121;
    byte[] val = Ints.toByteArray(testInt);
    value.put(val);
    memoryManager.put(0, key, value);

    ByteBuffer results = memoryManager.get(opID, key);
    int res = results.getInt();
    if (res == testInt) {
      System.out.println("true");
    } else {
      allPassed = false;
      System.out.println("false");
    }
    return true;
  }
}
