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
package edu.iu.dsc.tws.executor.comm;

import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.task.api.IMessage;

public abstract class ParallelOperation implements IParallelOperation {
  @Override
  public void send(int source, IMessage message) {
  }

  @Override
  public void send(int source, IMessage message, int dest) {
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {

  }

  @Override
  public void progress() {
  }
}
