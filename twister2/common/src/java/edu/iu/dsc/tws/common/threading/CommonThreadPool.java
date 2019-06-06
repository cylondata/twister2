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
package edu.iu.dsc.tws.common.threading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public final class CommonThreadPool {

  private static final String COMMON_POOL_THREADS = "twister2.common.thread.pool.threads";
  private static final String COMMON_POOL_KEEP_ALIVE = "twister2.common.thread.pool.keepalive";

  private static final Logger LOG = Logger.getLogger(CommonThreadPool.class.getName());
  private static volatile CommonThreadPool commonThreadPool;
  private ExecutorService executorService;
  private int threads;

  public static void init(Config config) {
    int threads = config.getIntegerValue(COMMON_POOL_THREADS, 1);
    int keepAlive = config.getIntegerValue(COMMON_POOL_KEEP_ALIVE, 10);
    if (commonThreadPool == null) {
      commonThreadPool = new CommonThreadPool(threads, keepAlive);
      LOG.fine("Initialized common thread pool with " + threads + " threads.");
    } else {
      LOG.warning("Thread pool has already initialized.");
    }
  }

  private CommonThreadPool(int thread, long keepAlive) {
    this.threads = thread;
    final AtomicInteger threadCount = new AtomicInteger(0);
    if (thread > 0) {
      this.executorService = new ThreadPoolExecutor(0, thread, keepAlive,
          TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
          r -> new Thread(r, "twister2-common-thread-pool-" + threadCount.getAndIncrement()));
    }
  }

  public static ExecutorService getExecutor() {
    if (commonThreadPool == null) {
      throw new RuntimeException("Twister2 common thread pool has not initialized");
    } else if (commonThreadPool.threads == 0) {
      throw new RuntimeException("Requested common thread pool executor which is "
          + "configured with 0 threads");
    } else {
      return commonThreadPool.executorService;
    }
  }

  public static int getThreadCount() {
    if (commonThreadPool == null) {
      throw new RuntimeException("Twister2 common thread pool has not initialized");
    } else {
      return commonThreadPool.threads;
    }
  }
}
