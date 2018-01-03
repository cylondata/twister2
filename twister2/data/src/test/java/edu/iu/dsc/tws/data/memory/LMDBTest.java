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

import com.google.common.primitives.Longs;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;

/**
 * Created by pulasthi on 1/2/18.
 */
public class LMDBTest {
  public static void main(String[] args) {
    //Memory Manager
    Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase");
    MemoryManager memoryManager = new LMDBMemoryManager(dataPath);
    byte[] val = Longs.toByteArray(1231212121213L);
    memoryManager.put(1234L, val);

    ByteBuffer results = memoryManager.get(1234L);
    long res = results.getLong();
    if(res == 1231212121213L){
      System.out.println("Correct");
    }
  }
}
