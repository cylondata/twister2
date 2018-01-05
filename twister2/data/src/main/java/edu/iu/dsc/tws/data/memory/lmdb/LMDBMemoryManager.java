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
import java.util.logging.Logger;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.AbstractMemoryManager;

import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;

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
   * The Database for the Memory Manager
   */
  private Dbi<ByteBuffer> db;

  public LMDBMemoryManager(Path dataPath) {
    this.lmdbDataPath = dataPath;
    init();
  }

  @Override
  public boolean init() {
    if (lmdbDataPath == null || lmdbDataPath.isNullOrEmpty()) {
      lmdbDataPath = new Path(LMDBMemoryManagerContext.DEFAULT_FOLDER_PATH);
    }
    final File path = new File(lmdbDataPath.getPath());

    this.env = create()
        .setMapSize(LMDBMemoryManagerContext.MAP_SIZE_LIMIT)
        .setMaxDbs(LMDBMemoryManagerContext.MAX_DB_INSTANCES)
        .setMaxReaders(LMDBMemoryManagerContext.MAX_READERS)
        .open(path);

    // The database supports duplicate values for a single key
    db = env.openDbi(LMDBMemoryManagerContext.DB_NAME, MDB_CREATE, MDB_DUPSORT);

    return true;
  }

  @Override
  public ByteBuffer get(ByteBuffer key) {
    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }
    // details in LMDB for clarity
    // To fetch any data from LMDB we need a Txn. A Txn is very important in
    // LmdbJava because it offers ACID characteristics and internally holds a
    // read-only key buffer and read-only value buffer. These read-only buffers
    // are always the same two Java objects, but point to different LMDB-managed
    // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
    // valid only until the Txn is released or the next Dbi or Cursor call. If
    // you need data afterwards, you should copy the bytes to your own buffer.
    //TODO: does the value returned from db.get and tnx.val() have the same data? need to check
    Txn<ByteBuffer> txn = env.txnRead();

    ByteBuffer result = db.get(txn, key);
    txn.close();
    return result;
  }

  @Override
  public ByteBuffer get(byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return get(keyBuffer);
  }

  @Override
  public ByteBuffer get(long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return get(keyBuffer);
  }

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

  @Override
  public byte[] getBytes(byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return getBytes(keyBuffer);
  }

  @Override
  public byte[] getBytes(long key) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return getBytes(key);
  }

  @Override
  public byte[] getBytes(ByteBuffer key) {

    if (key.position() != 0) {
      key.flip();
    }

    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return null;
    }

    Txn<ByteBuffer> txn = env.txnRead();
    final ByteBuffer found = db.get(txn, key);
    byte[] results = new byte[found.limit()];
    found.get(results);
    return results;
  }

  @Override
  public boolean containsKey(ByteBuffer key) {
    if (key.limit() > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }
    Txn<ByteBuffer> txn = env.txnRead();
    final ByteBuffer found = db.get(txn, key);

    if (found == null) {
      return false;
    }
    return true;
  }

  @Override
  public boolean containsKey(byte[] key) {
    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key).flip();

    return containsKey(keyBuffer);
  }

  @Override
  public boolean containsKey(long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);

    return containsKey(keyBuffer);
  }

  @Override
  public boolean append(ByteBuffer key, ByteBuffer value) {
    ByteBuffer results = get(key);
    if (results == null) {
      return put(key, value);
    }

    int capacity = results.limit() + value.limit();
    ByteBuffer appended = ByteBuffer.allocateDirect(capacity)
        .put(results)
        .put(value);
    return put(key, appended);
  }

  @Override
  public boolean append(long key, ByteBuffer value) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return append(keyBuffer, value);
  }

  /**
   * Insert key value pair into the
   *
   * @param key the key, must be unver 511 bytes because of limits in LMDB implementaion
   * @param value the value to be added
   * @return true if value was added, false otherwise
   */
  public boolean put(ByteBuffer key, ByteBuffer value) {
    if (db == null) {
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
    db.put(key, value);
    return true;
  }

  @Override
  public boolean put(byte[] key, byte[] value) {

    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    final ByteBuffer valBuffer = allocateDirect(value.length);
    keyBuffer.put(key);
    valBuffer.put(value);
    return put(keyBuffer, valBuffer);
  }

  @Override
  public boolean put(byte[] key, ByteBuffer value) {

    if (key.length > 511) {
      LOG.info("Key size lager than 511 bytes which is the limit for LMDB key values");
      return false;
    }

    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return put(keyBuffer, value);
  }

  @Override
  public boolean put(long key, ByteBuffer value) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return put(keyBuffer, value);
  }

  @Override
  public boolean put(long key, byte[] value) {

    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    final ByteBuffer valBuffer = allocateDirect(value.length);
    keyBuffer.putLong(0, key);
    valBuffer.put(value);
    return put(keyBuffer, valBuffer);
  }

  @Override
  public boolean delete(ByteBuffer key) {
    if (db == null) {
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

    return db.delete(key);
  }

  @Override
  public boolean delete(byte[] key) {
    final ByteBuffer keyBuffer = allocateDirect(key.length);
    keyBuffer.put(key);
    return delete(keyBuffer);
  }

  @Override
  public boolean delete(long key) {
    final ByteBuffer keyBuffer = allocateDirect(Long.BYTES);
    keyBuffer.putLong(0, key);
    return delete(keyBuffer);
  }

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
}
