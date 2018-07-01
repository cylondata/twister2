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
package edu.iu.dsc.tws.executor.core;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class ExecutorContext extends Context {
  public static final String THREADS_PER_WORKER = "twister2.exector.worker.threads";

  public static int threadsPerContainer(Config cfg) {
    String numOfThreads = SchedulerContext.numOfThreads(cfg);
    return Integer.parseInt(numOfThreads);
  }

}
