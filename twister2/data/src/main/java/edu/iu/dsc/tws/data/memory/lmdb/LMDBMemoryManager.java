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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.AbstractMemoryManager;
import edu.iu.dsc.tws.data.memory.MemoryManagerContext;
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import edu.iu.dsc.tws.data.utils.MemoryDeserializer;


import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_WRITEMAP;

/**
 * Memory Manger implementaion for LMDB Java
 * https://github.com/lmdbjava/lmdbjava
 */
public class LMDBMemoryManager extends AbstractMemoryManager {

  private static final Logger LOG = Logger.getLogger(LMDBMemoryManager.class.getName());

  /**
   * Path to keep the memory mapped file for the Memory manager
   */
  private Path lmdbDataPath;

  /**
   * The Memory Manager environment
   */
  private Env<ByteBuffer> env;

  /**
   * The Default Database for the Memory Manager
   * This can be used to store data that is not realated to any operation
   */
  private Dbi<ByteBuffer> db;

  private Map<Integer, Dbi<ByteBuffer>> dbMap;

  private Map<Integer, LinkedBlockingDeque<Pair<byte[], byte[]>>> dataQueueMap;
  protected Lock lock = new ReentrantLock();

  private ByteBuffer keyBuffer;
  private ByteBuffer dataBuffer;

  protected static Boolean needsCommitWriter;
  protected static Boolean needsCommitReader;


  /**
   * Stack of read Txn used for reads on this executor.
   * TODO: need to allow mutiple readears
   */
  private Stack<Txn<ByteBuffer>> readTxns;

  private ThreadLocal<Txn<ByteBuffer>> threadReadTxn;
  //  private ThreadLocal<Txn<ByteBuffer>> threadWriteTxn;
//  private ThreadLocal<Cursor<ByteBuffer>> threadWriteCursor;
//  private ThreadLocal<Boolean> threadNeedCommit;
  private ThreadLocal<ByteBuffer> threadappendBuffer;

  public LMDBMemoryManager(Path dataPath) {
    this.lmdbDataPath = dataPath;
    init();
  }

  @Override
  public boolean init() {
    try {

      if (lmdbDataPath == null || lmdbDataPath.isNullOrEmpty()) {
        lmdbDataPath = new Path(LMDBMemoryManagerContext.DEFAULT_FOLDER_PATH);
      }
      final File path = new File(lmdbDataPath.getPath());
      if (!path.exists()) {
        path.mkdirs();
      }

      final EnvFlags[] envFlags = envFlags(true, false);
      this.env = create()
          .setMapSize(LMDBMemoryManagerContext.MAP_SIZE_LIMIT)
          .setMaxDbs(LMDBMemoryManagerContext.MAX_DB_INSTANCES)
          .setMaxReaders(LMDBMemoryManagerContext.MAX_READERS)
          .open(path, envFlags);

      // The database supports duplicate values for a single key
      db = env.openDbi(LMDBMemoryManagerContext.DB_NAME, MDB_CREATE);
      dbMap = new HashMap<Integer, Dbi<ByteBuffer>>();
      keyBuffer = ByteBuffer.allocateDirect(LMDBMemoryManagerContext.KEY_BUFF_INIT_CAP);
      dataBuffer = ByteBuffer.allocateDirect(LMDBMemoryManagerContext.DATA_BUFF_INIT_CAP);
      dataQueueMap = new HashMap<>();

      LMDBMemoryManager.needsCommitReader = false;
      LMDBMemoryManager.needsCommitWriter = true;
      Thread writerThread = new Thread(new LMDBDataWriter(dbMap, dataQueueMap, env));
      writerThread.start();
      //populate readTxnStack
//      readTxns = new Stack<>();
//      for (int i = 0; i < 10; i++) {
//        readTxns.push(env.txnRead());
//      }

      threadReadTxn = new ThreadLocal<>();
//      threadWriteTxn = new ThreadLocal<>();
//      threadWriteCursor = new ThreadLocal<>();
//      threadNeedCommit = new ThreadLocal<>();
      threadappendBuffer = new ThreadLocal<>();
    } catch (RuntimeException e) {
      throw new RuntimeException("Error while creating LMDB database at Path "
          + lmdbDataPath.toString(), e);
    }

    return true;
  }

  @Override
  public ByteBuffer get(int opID, ByteBuffer key) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return null;
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    if (threadReadTxn.get() == null) {
      threadReadTxn.set(env.txnRead());
    }
//    if (threadNeedCommit.get() != null && threadNeedCommit.get()) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    // details in LMDB for clarity
    // To fetch any data from LMDB we need a Txn. A Txn is very important in
    // LmdbJava because it offers ACID characteristics and internally holds a
    // read-only key buffer and read-only value buffer. These read-only buffers
    // are always the same two Java objects, but point to different LMDB-managed
    // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
    // valid only until the Txn is released or the next Dbi or Cursor call. If
    // you need data afterwards, you should copy the bytes to your own buffer.
    //TODO: does the value returned from db.get and tnx.val() have the same data? need to check
//    Txn<ByteBuffer> txn = requestReadTxn();
    Txn<ByteBuffer> txn = threadReadTxn.get();
    txn.reset();
    txn.renew();
    ByteBuffer result = currentDB.get(txn, key);
//    releaseReadTxn(txn);
    return result;
  }

  /*@Override
  public ByteBuffer get(int opID, byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return get(opID, keyBuffer);
  }

  @Override
  public ByteBuffer get(int opID, long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return get(opID, keyBuffer);
  }*/

  @Override
  public ByteBuffer get(int opID, String key) {
    ByteBuffer temp = MemoryManagerContext.DEFAULT_CHARSET.encode(key);
    ByteBuffer keyBuffertemp = ByteBuffer.allocateDirect(temp.limit());
    keyBuffertemp.put(temp);
    return get(opID, keyBuffertemp);
  }

  /*@Override
  public <T extends Serializable> ByteBuffer get(int opID, T key) {
    return null;
  }*/

  public ByteBuffer getAll(ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }
    return null;
  }

  /*@Override
  public byte[] getBytes(int opID, byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return getBytes(opID, keyBuffer);
  }

  @Override
  public byte[] getBytes(int opID, long key) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return getBytes(opID, key);
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
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return null;
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    Txn<ByteBuffer> txn = env.txnRead();
    final ByteBuffer found = currentDB.get(txn, key);
    byte[] results = new byte[found.limit()];
    found.get(results);
    return results;
  }*/

  @Override
  public boolean containsKey(int opID, ByteBuffer key) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return false;
    }
    if (key.position() != 0) {
      key.flip();
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }
    if (threadReadTxn.get() == null) {
      threadReadTxn.set(env.txnRead());
    }
//    if (threadNeedCommit.get() != null && threadNeedCommit.get()) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    Txn<ByteBuffer> txn = threadReadTxn.get();
    txn.reset();
    txn.renew();
    final ByteBuffer found = currentDB.get(txn, key);
//    releaseReadTxn(txn);
    if (found == null) {
      return false;
    }
    return true;
  }

  /*@Override
  public boolean containsKey(int opID, byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key).flip();

    return containsKey(opID, keyBuffer);
  }

  @Override
  public boolean containsKey(int opID, long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);

    return containsKey(opID, keyBuffer);
  }*/

  @Override
  public boolean containsKey(int opID, String key) {
    ByteBuffer temp = MemoryManagerContext.DEFAULT_CHARSET.encode(key);
    ByteBuffer keyBuffertemp = ByteBuffer.allocateDirect(temp.limit());
    keyBuffertemp.put(temp);
    return containsKey(opID, keyBuffertemp);
  }

  /*@Override
  public <T extends Serializable> boolean containsKey(int opID, T key) {
    return false;
  }*/

  @Override
  public boolean append(int opID, ByteBuffer key, ByteBuffer value) {
    ByteBuffer results = get(opID, key);
    if (value.position() != 0) {
      value.flip();
    }
    if (results == null) {
      return put(opID, key, value);
    }

    int capacity = results.limit() + value.limit();
    if (threadappendBuffer.get() == null) {
      threadappendBuffer.set(ByteBuffer.allocateDirect(
          LMDBMemoryManagerContext.DEFAULT_APPEND_BUFFER_SIZE));
    }
    if (capacity > threadappendBuffer.get().capacity()) {
      threadappendBuffer.set(ByteBuffer.allocateDirect(capacity * 2));
    }
    threadappendBuffer.get().clear();
    threadappendBuffer.get()
        .put(results)
        .put(value);
    return put(opID, key, threadappendBuffer.get());
  }

  /*@Override
  public boolean append(int opID, byte[] key, ByteBuffer value) {
    return false;
  }

  @Override
  public boolean append(int opID, long key, ByteBuffer value) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return append(opID, keyBuffer, value);
  }*/

  @Override
  public boolean append(int opID, String key, ByteBuffer value) {
    ByteBuffer temp = MemoryManagerContext.DEFAULT_CHARSET.encode(key);
    ByteBuffer keyBuffertemp = ByteBuffer.allocateDirect(temp.limit());
    keyBuffertemp.put(temp);
    return append(opID, keyBuffertemp, value);
  }

  /*@Override
  public <T extends Serializable> boolean append(int opID, T key, ByteBuffer value) {
    return false;
  }*/

  /**
   * Insert key value pair into the
   *
   * @param key the key, must be unver 511 bytes because of limits in LMDB implementaion
   * @param value the value to be added
   * @return true if value was added, false otherwise
   */
  public boolean put(int opID, ByteBuffer key, ByteBuffer value) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return false;
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (currentDB == null) {
      throw new RuntimeException("LMDB database has not been configured."
          + " Please initialize database");
    }

    if (key.position() != 0) {
      key.flip();
    }

    if (value.position() != 0) {
      value.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }
//    if (threadWriteTxn.get() == null) {
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }

//    threadWriteCursor.get().put(key, value);
    //  threadNeedCommit.set(true);
    currentDB.put(key, value);
    return true;
  }

  /**
   * Insert key value pair into the
   *
   * @param key the key, must be unver 511 bytes because of limits in LMDB implementaion
   * @param value the value to be added
   * @return true if value was added, false otherwise
   */
  public boolean put(int opID, byte[] key, byte[] value) {
//    if (!dbMap.containsKey(opID)) {
//      LOG.info("The given operation does not have a corresponding store specified");
//      return false;
//    }
//
//    if (key.length > 511) {
//      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
//      return false;
//    }
    try {
      dataQueueMap.get(opID).put(new ImmutablePair<>(key, value));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Insert bulk key value pair into the
   *
   * @param keys the keys, must be unver 511 bytes because of limits in LMDB implementaion
   * @param values the values to be added
   * @return true if value was added, false otherwise
   */
  public boolean put(int opID, List<ByteBuffer> keys, List<ByteBuffer> values) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return false;
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (currentDB == null) {
      throw new RuntimeException("LMDB database has not been configured."
          + " Please initialize database");
    }
    try (Txn<ByteBuffer> tx = env.txnWrite();) {
      try (Cursor<ByteBuffer> c = currentDB.openCursor(tx);) {
        for (int i = 0; i < keys.size(); i++) {
          keys.get(i).rewind();
          if (keys.get(i).limit() > 511) {
            LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
            return false;
          }
          values.get(i).rewind();
          c.put(keys.get(i), values.get(i));
        }
      } catch (RuntimeException e) {
        throw new RuntimeException("Error while creating cursor", e);
      }
      tx.commit();
    } catch (RuntimeException e) {
      throw new RuntimeException("Error while creating txn", e);
    }
    return true;
  }

  /*@Override
  public boolean put(int opID, byte[] key, byte[] value) {

    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    final ByteBuffer valBuffer = allocateDirect(value.length);
    keyBuffer.put(key);
    valBuffer.put(value);
    return put(opID, keyBuffer, valBuffer);
  }

  @Override
  public boolean put(int opID, byte[] key, ByteBuffer value) {

    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return put(opID, keyBuffer, value);
  }

  @Override
  public boolean put(int opID, long key, ByteBuffer value) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return put(opID, keyBuffer, value);
  }*/

  @Override
  public boolean put(int opID, String key, ByteBuffer value) {
    ByteBuffer temp = MemoryManagerContext.DEFAULT_CHARSET.encode(key);
    ByteBuffer keyBuffertemp = ByteBuffer.allocateDirect(temp.limit());
    keyBuffertemp.put(temp);
    return put(opID, keyBuffertemp, value);
  }

  /*@Override
  public <T extends Serializable> boolean put(int opID, T key, ByteBuffer value) {
    return false;
  }

  @Override
  public boolean put(int opID, long key, byte[] value) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    final ByteBuffer valBuffer = allocateDirect(value.length);
    keyBuffer.putLong(0, key);
    valBuffer.put(value);
    return put(opID, keyBuffer, valBuffer);
  }

  @Override
  public boolean put(int opID, String key, byte[] value) {
    return false;
  }

  @Override
  public <T extends Serializable> boolean put(int opID, T key, byte[] value) {
    return false;
  }*/

  @Override
  public boolean delete(int opID, ByteBuffer key) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return false;
    }
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
    if (currentDB == null) {
      throw new RuntimeException("LMDB database has not been configured."
          + " Please initialize database");
    }

    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }
//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//    }
    currentDB.delete(key);

//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    return true;
  }

  /*@Override
  public boolean delete(int opID, byte[] key) {
    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return delete(opID, keyBuffer);
  }

  @Override
  public boolean delete(int opID, long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return delete(opID, keyBuffer);
  }*/

  @Override
  public boolean delete(int opID, String key) {
    //TODO check if there is better method to get direct byte buffer from String
    ByteBuffer temp = MemoryManagerContext.DEFAULT_CHARSET.encode(key);
    ByteBuffer keyBuffertemp = ByteBuffer.allocateDirect(temp.limit());
    keyBuffertemp.put(temp);
    return delete(opID, keyBuffertemp);
  }

  /*@Override
  public <T extends Serializable> boolean delete(int opID, T key) {
    return false;
  }*/

  /**
   * At this level the method does not return an OperationMemoryManager since the implementaion
   * does not handle OperationMemoryManager's
   */
  @Override
  public OperationMemoryManager addOperation(int opID, DataMessageType messageType) {
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);

//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//    }

    dataQueueMap.put(opID, new LinkedBlockingDeque<>(
        LMDBMemoryManagerContext.DEFAULT_WRITE_BUFFER_MAP_SIZE));
    dbMap.put(opID, env.openDbi(String.valueOf(opID), MDB_CREATE));
//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }

    return new OperationMemoryManager(opID, messageType, this);
  }

  @Override
  public OperationMemoryManager addOperation(int opID, DataMessageType messageType,
                                             DataMessageType keyType) {
//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//    }
    dataQueueMap.put(opID, new LinkedBlockingDeque<>(
        LMDBMemoryManagerContext.DEFAULT_WRITE_BUFFER_MAP_SIZE));
    dbMap.put(opID, env.openDbi(String.valueOf(opID), MDB_CREATE));

//    if (threadNeedCommit.get() != null) {
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    return new OperationMemoryManager(opID, messageType, keyType, this);
  }

  @Override
  public boolean removeOperation(int opID) {
    //TODO: lmdb docs say that calling close is normally unnecessary; use with caution. need to
    // later check this to make sure it has been done the correct way
    dbMap.get(opID).close();
    dbMap.remove(opID);
    return true;
  }

  @Override
  public boolean flush(int opID, ByteBuffer key) {
    return false;
  }

  public boolean flush(int opID) {
    //if this was called from put we already have the lock so no need to aquire again
//    boolean gotlock = lock.tryLock();
//    Dbi<ByteBuffer> currentDB = dbMap.get(opID);
//    Deque<Pair<byte[], byte[]>> temp = bufferMap.get(opID);
//    try (Txn<ByteBuffer> tx = env.txnWrite();) {
//      try (Cursor<ByteBuffer> c = currentDB.openCursor(tx)) {
//        Pair<byte[], byte[]> tempPair = null;
//        while ((tempPair = temp.poll()) != null) {
//          setupThreadLocalBuffers(tempPair.getKey().length, tempPair.getValue().length);
//          keyBuffer.put(tempPair.getKey());
//          keyBuffer.flip();
//
//          dataBuffer.putInt(tempPair.getValue().length);
//          dataBuffer.put(tempPair.getValue());
//          dataBuffer.flip();
//
//          c.put(keyBuffer, dataBuffer);
//        }
//      } catch (RuntimeException e) {
//        throw new RuntimeException("Error while creating cursor", e);
//      }
//      tx.commit();
//    } catch (RuntimeException e) {
//      throw new RuntimeException("Error while creating txn", e);
//    }
//    if (gotlock) {
//      lock.unlock();
//    }
    return true;
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

  /*@Override
  public boolean flush(int opID, byte[] key) {
    return false;
  }

  @Override
  public boolean flush(int opID, long key) {
    return false;
  }*/

  @Override
  public boolean flush(int opID, String key) {
    return false;
  }

  /*@Override
  public <T extends Serializable> boolean flush(int opID, T key) {
    return false;
  }*/

  @Override
  public boolean close(int opID, ByteBuffer key) {
    return false;
  }

 /* @Override
  public boolean close(int opID, byte[] key) {
    return false;
  }

  @Override
  public boolean close(int opID, long key) {
    return false;
  }*/

  @Override
  public boolean close(int opID, String key) {
    return false;
  }

  /**
   * Returns an iterator that contains all the byte buffers for the given operation
   */
  @Override
  public Iterator<Object> getIterator(int opID, DataMessageType keyType,
                                      DataMessageType valueType,
                                      KryoMemorySerializer deSerializer, ByteOrder order) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return null;
    }
    if (threadReadTxn.get() == null) {
      threadReadTxn.set(env.txnRead());
    }
    List<Object> results = new ArrayList<>(50000);
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);

//    if (threadNeedCommit.get() != null && threadNeedCommit.get()) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    lock.lock();
    if (needsCommitWriter) {
      needsCommitReader = true;
      while (needsCommitReader) {
        // busy wait till commit is done
      }
    }
    lock.unlock();
    int limit = 50000;
    int tempCount = 0;
    boolean firstd = true;
    Txn<ByteBuffer> txn = threadReadTxn.get();
    txn.reset();
    txn.renew();
    try (CursorIterator<ByteBuffer> it = currentDB.iterate(txn, KeyRange.all())) {
      for (final CursorIterator.KeyVal<ByteBuffer> kv : it.iterable()) {
        Object key = MemoryDeserializer.deserializeKey(kv.key().order(order),
            keyType, deSerializer);
        Object value = MemoryDeserializer.deserializeValue(kv.val().order(order),
            valueType, deSerializer);
        if (firstd) {
          results.add(new ImmutablePair<>(key, value));
        } else {
          results.set(tempCount, new ImmutablePair<>(key, value));
        }
        tempCount++;
        if (tempCount >= limit) {
          tempCount = 0;
          firstd = false;
          break;
        }
      }
    }
//    releaseReadTxn(txn);
    return results.iterator();
  }

  /**
   * Returns an iterator that contains all the byte buffers for the given operation
   * This method assumes that the keys are int's and that the do not need to be returned
   */
  @Override
  public Iterator<Object> getIterator(int opID, DataMessageType valueType,
                                      KryoMemorySerializer deSerializer, ByteOrder order) {
    if (!dbMap.containsKey(opID)) {
      LOG.info("The given operation does not have a corresponding store specified");
      return null;
    }
    if (threadReadTxn.get() == null) {
      threadReadTxn.set(env.txnRead());
    }
    List<Object> results = new ArrayList<>();
    Dbi<ByteBuffer> currentDB = dbMap.get(opID);

//    if (threadNeedCommit.get() != null && threadNeedCommit.get()) {
//      threadWriteTxn.get().commit();
//      threadWriteTxn.get().close();
//
//      threadWriteTxn.set(env.txnWrite());
//      threadWriteCursor.set(currentDB.openCursor(threadWriteTxn.get()));
//      threadNeedCommit.set(false);
//    }
    lock.lock();
    if (needsCommitWriter) {
      needsCommitReader = true;
      while (needsCommitReader) {
        // busy wait till commit is done
      }
    }
    lock.unlock();

    Txn<ByteBuffer> txn = threadReadTxn.get();
    txn.reset();
    txn.renew();
    try (CursorIterator<ByteBuffer> it = currentDB.iterate(txn, KeyRange.all())) {
      for (final CursorIterator.KeyVal<ByteBuffer> kv : it.iterable()) {
        Object value = MemoryDeserializer.deserializeValue(kv.val().order(order),
            valueType, deSerializer);
        results.add(value);
      }
    }
//    releaseReadTxn(txn);
    return results.iterator();
  }

 /* @Override
  public <T extends Serializable> boolean close(int opID, T key) {
    return false;
  }*/

  public Path getLmdbDataPath() {
    return lmdbDataPath;
  }

  public void setLmdbDataPath(Path lmdbDataPath) {
    this.lmdbDataPath = lmdbDataPath;
  }


  public Env<ByteBuffer> getEnv() {
    return env;
  }

  public void setEnv(Env<ByteBuffer> env) {
    this.env = env;
  }

  public Dbi<ByteBuffer> getDb() {
    return db;
  }

  public void setDb(Dbi<ByteBuffer> db) {
    this.db = db;
  }

  static final EnvFlags[] envFlags(final boolean writeMap, final boolean sync) {
    final Set<EnvFlags> envFlagSet = new HashSet<>();
    if (writeMap) {
      envFlagSet.add(MDB_WRITEMAP);
      envFlagSet.add(EnvFlags.MDB_NOTLS);
      //TODO: need to test with other flags if no sync is used will need env.sync(true) to sync
//            envFlagSet.add(EnvFlags.MDB_NOSYNC);
//            envFlagSet.add(EnvFlags.MDB_NOMETASYNC);
//            envFlagSet.add(MDB_MAPASYNC);

    }
    if (!sync) {
      envFlagSet.add(MDB_NOSYNC);
    }
    final EnvFlags[] envFlags = new EnvFlags[envFlagSet.size()];
    envFlagSet.toArray(envFlags);
    return envFlags;
  }

  private synchronized Txn<ByteBuffer> requestReadTxn() {
    if (readTxns.isEmpty()) {
      throw new RuntimeException("No read Txn avilable");
    }
    return readTxns.pop();
  }

  private synchronized void releaseReadTxn(Txn<ByteBuffer> txn) {
    txn.commit(); // Might not need commit can check and remove
    txn.reset();
    txn.renew();
    readTxns.push(txn);
  }
}
