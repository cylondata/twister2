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
package edu.iu.dsc.tws.executor.comms;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.executor.IParallelOperation;
import edu.iu.dsc.tws.api.compute.executor.ISync;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;

public abstract class AbstractParallelOperation implements IParallelOperation {

  protected Config config;

  protected Communicator channel;

  protected Map<Integer, BlockingQueue<IMessage>> outMessages = new HashMap<>();

  protected Map<Integer, ISync> syncs = new HashMap<>();

  protected LogicalPlan logicalPlan;

  protected String inEdge;

  public AbstractParallelOperation(Config config, Communicator network,
                                   LogicalPlan tPlan, String edge) {
    this.config = config;
    this.logicalPlan = tPlan;
    this.channel = network;
    this.inEdge = edge;
  }

  public void registerSync(int targetTask, ISync sink) {
    syncs.put(targetTask, sink);
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {
    if (outMessages.containsKey(targetTask)) {
      throw new RuntimeException("Existing queue for target task");
    }
    outMessages.put(targetTask, queue);
  }

  public boolean sendBarrier(int src, long barrierId) {
    return this.getOp().sendBarrier(src, barrierId);
  }

  @Override
  public void close() {
    this.getOp().close();
  }

  @Override
  public void reset() {
    this.getOp().reset();
  }

  @Override
  public boolean isComplete() {
    // first progress
    this.progress();

    // then check isComplete
    return this.getOp().isComplete();
  }

  protected abstract BaseOperation getOp();

  @Override
  public void finish(int source) {
    this.getOp().finish(source);
  }

  @Override
  public boolean progress() {
    return this.getOp().progress();
  }
}
