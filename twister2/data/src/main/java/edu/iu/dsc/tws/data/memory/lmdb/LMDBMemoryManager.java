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

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.MemoryManager;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static java.nio.ByteBuffer.allocateDirect;

/**
 * Memory Manger implementaion for LMDB Java
 * https://github.com/lmdbjava/lmdbjava
 */
public class LMDBMemoryManager implements MemoryManager {

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
  Dbi<ByteBuffer> db;

  public LMDBMemoryManager(Path dataPath) {
    this.lmdbDataPath = dataPath;
  }

  @Override
  public boolean init() {
    File path = new File(lmdbDataPath.getPath());

    this.env = create()
        .setMapSize(LMDBMemoryManagerContext.MAP_SIZE_LIMIT)
        .setMaxDbs(LMDBMemoryManagerContext.MAX_DB_INSTANCES)
        .open(path);

    db = env.openDbi(LMDBMemoryManagerContext.DB_NAME, MDB_CREATE);

    return false;
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
    keyBuffer.put(key).flip();
    keyBuffer.put(value).flip();
    db.put(keyBuffer, valBuffer);
    return true;
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
