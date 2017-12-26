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
  public static final long MAP_SIZE_LIMIT = 1024*1024*1024*2;

  /**
   * specifies the maximum number of DBs will be created in 1 environment.
   * value set to 1
   */
  public static final int MAX_DB_INSTANCES = 1;

  /**
   * Name of the databse
   */
  public static final String DB_NAME = "default_twister2_lmdb";
}
