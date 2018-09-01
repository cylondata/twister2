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
package edu.iu.dsc.tws.executor.threading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.ExecutorContext;

public abstract class ThreadSharingExecutor extends AbstractExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  protected int numThreads;

  protected BlockingQueue<INodeInstance> tasks;

  protected List<Thread> threads = new ArrayList<>();

  protected ExecutionPlan executionPlan;

  protected TWSChannel channel;

  protected Config config;

  public boolean execute(Config cfg, ExecutionPlan plan, TWSChannel ch) {
    this.numThreads = ExecutorContext.threadsPerContainer(cfg);
    this.channel = ch;
    this.executionPlan = plan;
    this.config = cfg;
    // go through the instances
    return runExecution();
  }

  public abstract boolean runExecution();

  public void progressStreamComm() {
    while (true) {
      this.channel.progress();
    }
  }

  public boolean isDone() {
    return false;
  }
}
