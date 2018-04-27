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
package edu.iu.dsc.tws.data.memory.lmdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

/**
 * Created by pulasthi on 3/16/18.
 */
public class LMDBDataWriter implements Runnable {
  private ByteBuffer keyBuffer;
  private ByteBuffer dataBuffer;
  private int currentDbiCount;

  private Env<ByteBuffer> envRef;
  private Map<Integer, Dbi<ByteBuffer>> dbMapRef;
  private Map<Integer, LinkedBlockingDeque<Pair<byte[], byte[]>>> dataQueueMapRef;

  public LMDBDataWriter(Map<Integer, Dbi<ByteBuffer>> dbMap, Map<Integer,
      LinkedBlockingDeque<Pair<byte[], byte[]>>> dataQueueMap, Env<ByteBuffer> env) {
    this.dbMapRef = dbMap;
    this.dataQueueMapRef = dataQueueMap;
    this.envRef = env;

    this.keyBuffer = ByteBuffer.allocateDirect(LMDBMemoryManagerContext.KEY_BUFF_INIT_CAP);
    this.dataBuffer = ByteBuffer.allocateDirect(LMDBMemoryManagerContext.DATA_BUFF_INIT_CAP);
    this.currentDbiCount = 0;

  }

  @Override
  public void run() {

    Txn<ByteBuffer> writeTxn = null;
    Map<Integer, Cursor<ByteBuffer>> openCoursors = new HashMap<>();
    List<Integer> opIDs = new ArrayList<>();
    int opID = 0;
    Cursor<ByteBuffer> c = null;
    int count = 0;
    while (currentDbiCount == 0) {
      if (dbMapRef.size() > 0 && dataQueueMapRef.size() > 0) {
        writeTxn = this.envRef.txnWrite();
        for (Map.Entry<Integer, Dbi<ByteBuffer>> dbiEntry : dbMapRef.entrySet()) {
          opID = dbiEntry.getKey();
          c = dbiEntry.getValue().openCursor(writeTxn);
          currentDbiCount++;
        }
      }
    }

    Pair<byte[], byte[]> tempPair = null;
    LinkedBlockingDeque<Pair<byte[], byte[]>> dataQueue = dataQueueMapRef.get(opID);

    while (true) {

//      if (dbMapRef.size() > currentDbiCount) {
//        for (Map.Entry<Integer, Dbi<ByteBuffer>> dbiEntry : dbMapRef.entrySet()) {
//          if (!openCoursors.containsKey(dbiEntry.getKey())) {
//            openCoursors.put(dbiEntry.getKey(), dbiEntry.getValue().openCursor(writeTxn));
//            opIDs.add(dbiEntry.getKey());
//            currentDbiCount++;
//          }
//        }
//      }
//
//      if(currentDbiCount == 0){
//        Thread.yield();
//      }
      tempPair = dataQueue.poll();
      if (tempPair != null) {
        setupThreadLocalBuffers(tempPair.getKey().length, tempPair.getValue().length);
        keyBuffer.put(tempPair.getKey());
        keyBuffer.flip();

        dataBuffer.putInt(tempPair.getValue().length);
        dataBuffer.put(tempPair.getValue());
        dataBuffer.flip();

        c.put(keyBuffer, dataBuffer);
        LMDBMemoryManager.needsCommitWriter = true;
        count++;
        if (count % 2000000 == 0 && count > 0) {
          writeTxn.commit();
          writeTxn.close();
          writeTxn = this.envRef.txnWrite();
          c = dbMapRef.get(opID).openCursor(writeTxn);
          LMDBMemoryManager.needsCommitWriter = false;
          LMDBMemoryManager.needsCommitReader = false;
          System.out.println("added 2  mil to mm");
        }
      }

      if (LMDBMemoryManager.needsCommitReader) {
        writeTxn.commit();
        writeTxn.close();
        writeTxn = this.envRef.txnWrite();
        c = dbMapRef.get(opID).openCursor(writeTxn);
        LMDBMemoryManager.needsCommitWriter = false;
        LMDBMemoryManager.needsCommitReader = false;
      }

    }


  }

  private void setupThreadLocalBuffers(int keyLength, int dataLength) {
    int dataLengthTemp = dataLength + 4;
    if (keyBuffer.capacity() < keyLength) {
      keyBuffer = ByteBuffer.allocateDirect(keyLength);
    }
    if (dataBuffer.capacity() < dataLengthTemp) {
      dataBuffer = ByteBuffer.allocateDirect(dataLengthTemp);
    }

    dataBuffer.clear();
    keyBuffer.clear();
  }
}
