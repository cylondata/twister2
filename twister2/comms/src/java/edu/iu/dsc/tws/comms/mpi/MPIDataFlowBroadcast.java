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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowBroadCast;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.core.InstancePlan;
import edu.iu.dsc.tws.comms.utils.BinaryTree;
import edu.iu.dsc.tws.comms.utils.Routing;

public class MPIDataFlowBroadcast implements DataFlowBroadCast, MessageListener {
  private Config config;
  private InstancePlan instancePlan;
  private List<Integer> sources;
  private List<Integer> destinations;
  private int stream;
  private Routing routing;
  private TWSMPIChannel channel;
  /**
   * The send buffers used by the operation
   */
  private Queue<MPIBuffer> buffers;

  @Override
  public void init(Config config, InstancePlan instancePlan, List<Integer> sources,
                   List<Integer> destinations, int stream) {
    this.config = config;
    this.instancePlan = instancePlan;
    this.sources = sources;
    this.destinations = destinations;
    this.stream = stream;

    this.buffers = new LinkedList<>();

    // lets create the routing needed
    BinaryTree tree = new BinaryTree();
    tree.init(config, sources, destinations, stream);

    routing = tree.routing();
  }

  /**
   * Setup the receives and send buffers
   */
  private void setupCommunication() {
    List<Integer> receiving = routing.getReceivingIds();

    for (Integer recv : receiving) {
      channel.receiveMessage(recv, this, stream);
    }

    // configure the send buffers
    int sendBufferSize = MPIContext.getSendBufferSize(config);
    int sendBufferCount = MPIContext.getSendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      MPIBuffer buffer = new MPIBuffer(sendBufferSize);
      buffers.offer(buffer);
    }
  }

  @Override
  public void partial(Message message) {

  }

  @Override
  public void finish() {

  }

  @Override
  public void complete(Message message) {

  }

  @Override
  public void broadcast(Message message) {

  }

  @Override
  public void onReceiveComplete(int id, MPIMessage message) {
    // we received a message, we need to determine weather we need to forward to another node
    // and process

  }

  @Override
  public void onSendComplete(int id, MPIRequest message) {

  }
}
