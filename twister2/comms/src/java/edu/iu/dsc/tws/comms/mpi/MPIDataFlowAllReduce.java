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
package edu.iu.dsc.tws.comms.mpi;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class MPIDataFlowAllReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowAllReduce.class.getName());

  private MPIDataFlowReduce reduce;

  private MPIDataFlowBroadcast broadcast;
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, MPIDataFlowReduce> reduceMap;

  // the partial receiver
  private MessageReceiver partialReceiver;

  // the final receiver
  private MessageReceiver finalReceiver;

  private TWSMPIChannel channel;

  private int executor;

  public MPIDataFlowAllReduce(TWSMPIChannel chnl,
                            Set<Integer> sources, Set<Integer> destination,
                            MessageReceiver finalRecv,
                            MessageReceiver partialRecv) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.partialReceiver = partialRecv;
    this.finalReceiver = finalRecv;
    this.reduceMap = new HashMap<>();
  }

  @Override
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {

  }

  @Override
  public boolean sendPartial(int source, Object message) {
    return false;
  }

  @Override
  public boolean send(int source, Object message) {
    return false;
  }

  @Override
  public boolean send(int source, Object message, int path) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message, int path) {
    return false;
  }

  @Override
  public void progress() {
    finalReceiver.progress();
    partialReceiver.progress();

    broadcast.progress();
    reduce.progress();
  }

  @Override
  public void close() {

  }
}
