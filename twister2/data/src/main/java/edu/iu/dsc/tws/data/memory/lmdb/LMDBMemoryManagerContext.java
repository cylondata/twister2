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

import edu.iu.dsc.tws.common.config.Context;

/**
 * Created by pulasthi on 12/26/17.
 */
public class LMDBMemoryManagerContext extends Context {
  /**
   * Map size for the database. This specifies how large the DB might be. Over-estimating is OK.
   * value set to 2GB
   */
  public static final long MAP_SIZE_LIMIT = 1048576L * 1024L * 5L;

  /**
   * specifies the maximum number of DBs will be created in 1 environment.
   * value set to 1
   */
  public static final int MAX_DB_INSTANCES = 10;

  /**
   * The maximum number of readers set for an env
   */
  public static final int MAX_READERS = 128;

  public static final int DEFAULT_APPEND_BUFFER_SIZE = 1024 * 2;

  public static final int DEFAULT_WRITE_BUFFER_MAP_SIZE = 5000000;

  public static final int DATA_BUFF_INIT_CAP = 128;

  public static final int KEY_BUFF_INIT_CAP = 16;

  /**
   * Name of the databse
   */
  public static final String DB_NAME = "default_twister2_lmdb";

  /**
   * File path to store the LMDB database
   */
  public static final String DEFAULT_FOLDER_PATH = "\tmp\twister2lmdb";


}
