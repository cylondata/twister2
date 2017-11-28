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
package edu.iu.dsc.tws.task.core;

import edu.iu.dsc.tws.common.config.Context;

/**
 * Configurations specific to executor
 */
public class ExecutorContext extends Context {
  /**
   * The size of the task pool assigned to the executors
   */
  public static final int EXECUTOR_CORE_POOL_SIZE = 4;

  /**
   * The maximum size of the task pool
   */
  public static final int EXECUTOR_MAX_POOL_SIZE = Integer.MAX_VALUE;

  /**
   * The keep alive time for the tasks that are not in the core pool
   */
  public static final long EXECUTOR_POOL_KEEP_ALIVE_TIME = 1000 * 60;

  /**
   * sync lock used in the TaskExecutorFixedThread
   */
  public static final Integer FIXED_EXECUTOR_LOCK = new Integer(1);

  public enum QueueType {
    INPUT,
    OUTPUT
  }

}
