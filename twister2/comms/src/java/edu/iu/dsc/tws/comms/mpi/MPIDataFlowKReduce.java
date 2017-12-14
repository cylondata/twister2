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

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.KeyedMessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class MPIDataFlowKReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowKeyedReduce.class.getName());
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, MPIDataFlowReduce> reduceMap;

  // the partial receiver
  private KeyedMessageReceiver partialReceiver;

  // the final receiver
  private KeyedMessageReceiver finalReceiver;

  public MPIDataFlowKReduce(TWSMPIChannel channel,
                                Set<Integer> sources, Set<Integer> destination,
                                KeyedMessageReceiver finalRecv, KeyedMessageReceiver partialRecv) {

    this.sources = sources;
    this.destinations = destination;
    this.partialReceiver = partialRecv;
    this.finalReceiver = finalRecv;
  }

  @Override
  public boolean send(int source, Object message) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean send(int source, Object message, int path) {
    MPIDataFlowOperation reduce = reduceMap.get(path);
    if (reduce == null) {
      throw new RuntimeException("Un-expected destination: " + path);
    }
    return reduce.send(source, message, path);
  }

  @Override
  public boolean sendPartial(int source, Object message, int path) {
    MPIDataFlowOperation reduce = reduceMap.get(path);
    if (reduce == null) {
      throw new RuntimeException("Un-expected destination: " + path);
    }
    return reduce.sendPartial(source, message, path);
  }

  @Override
  public void progress() {
    for (MPIDataFlowReduce reduce : reduceMap.values()) {
      reduce.progress();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {

  }

  @Override
  public boolean sendPartial(int source, Object message) {
    // now what we need to do
    throw new RuntimeException("Not implemented");
  }
}
