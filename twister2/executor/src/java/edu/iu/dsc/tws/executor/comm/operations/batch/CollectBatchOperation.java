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
package edu.iu.dsc.tws.executor.comm.operations.batch;

import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.task.api.IMessage;

/**
 * This is a special batch operation to collect the data from in-memory tasks
 */
public class CollectBatchOperation implements IParallelOperation {
  @Override
  public boolean send(int source, IMessage message, int flags) {
    return false;
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {

  }

  @Override
  public boolean progress() {
    return false;
  }
}
