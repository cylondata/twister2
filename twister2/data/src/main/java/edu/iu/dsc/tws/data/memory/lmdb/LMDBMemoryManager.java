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

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;

import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.MemoryManager;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

/**
 * Memory Manger implementaion for LMDB Java
 * https://github.com/lmdbjava/lmdbjava
 */
public class LMDBMemoryManager implements MemoryManager {
  /**
   * Path to keep the memory mapped file for the Memory manager
   */
  private Path lmdbDataPath;

  public LMDBMemoryManager(Path dataPath) {
    this.lmdbDataPath = dataPath;
  }

  @Override
  public boolean init() {
    File path = new File(lmdbDataPath.getPath());

    Env<ByteBuffer> env = create()
        .setMapSize(LMDBMemoryManagerContext.MAP_SIZE_LIMIT)
        .setMaxDbs(LMDBMemoryManagerContext.MAX_DB_INSTANCES)
        .open(path);

    Dbi<ByteBuffer> db = env.openDbi(LMDBMemoryManagerContext.DB_NAME, MDB_CREATE);

    return false;
  }

  public Path getLmdbDataPath() {
    return lmdbDataPath;
  }

  public void setLmdbDataPath(Path lmdbDataPath) {
    this.lmdbDataPath = lmdbDataPath;
  }
}
