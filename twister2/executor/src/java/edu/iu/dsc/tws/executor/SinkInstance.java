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
package edu.iu.dsc.tws.executor;

import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;

public class SinkInstance  implements INodeInstance {
  /**
   * The actual task executing
   */
  private ISink task;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> inQueue;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The globally unique task id
   */
  private int taskId;

  public SinkInstance(ISink task, BlockingQueue<IMessage> inQueue, Config config) {
    this.task = task;
    this.inQueue = inQueue;
    this.config = config;
  }

  public void prepare() {
    task.prepare(config);
  }

  public void execute() {
    while (!inQueue.isEmpty()) {
      IMessage m = inQueue.poll();

      task.execute(m);
    }
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }
}
