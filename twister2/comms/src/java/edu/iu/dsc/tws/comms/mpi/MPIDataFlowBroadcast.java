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
import edu.iu.dsc.tws.comms.api.MessageBuilder;
import edu.iu.dsc.tws.comms.api.MessageFormatter;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.InstancePlan;
import edu.iu.dsc.tws.comms.utils.BinaryTree;
import edu.iu.dsc.tws.comms.utils.Routing;

public class MPIDataFlowBroadcast implements DataFlowBroadCast, MPIMessageListener {
  private Config config;
  private InstancePlan instancePlan;
  private List<Integer> sources;
  private List<Integer> destinations;
  private int stream;
  private Routing routing;
  private TWSMPIChannel channel;
  private MessageReceiver receiver;
  private MessageFormatter formatter;
  private MessageBuilder builder;
  /**
   * The send sendBuffers used by the operation
   */
  private Queue<MPIBuffer> sendBuffers;

  @Override
  public void init(Config config, InstancePlan instancePlan, List<Integer> sources,
                   List<Integer> destinations, int stream, MessageReceiver receiver,
                   MessageFormatter formatter, MessageBuilder builder) {
    this.config = config;
    this.instancePlan = instancePlan;
    this.sources = sources;
    this.destinations = destinations;
    this.stream = stream;
    this.formatter = formatter;
    this.builder = builder;
    this.receiver = receiver;
    this.sendBuffers = new LinkedList<>();

    // lets create the routing needed
    BinaryTree tree = new BinaryTree();
    tree.init(config, sources, destinations, stream);

    routing = tree.routing();
  }

  /**
   * Setup the receives and send sendBuffers
   */
  private void setupCommunication() {
    List<Integer> receiving = routing.getReceivingIds();

    for (Integer recv : receiving) {
      channel.receiveMessage(recv, this, stream);
    }

    // configure the send sendBuffers
    int sendBufferSize = MPIContext.getSendBufferSize(config);
    int sendBufferCount = MPIContext.getSendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      MPIBuffer buffer = new MPIBuffer(sendBufferSize);
      sendBuffers.offer(buffer);
    }
  }

  @Override
  public void partial(Message message) {
    throw new UnsupportedOperationException("partial messages not supported by broadcast");
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException("partial messages not supported by broadcast");
  }

  @Override
  public void complete(Message message) {
    Object msgObj = builder.build(message);
    if (!(msgObj instanceof MPIMessage)) {
      throw new IllegalArgumentException("Expecting a message of MPIMessage type");
    }

    MPIMessage mpiMessage = (MPIMessage) msgObj;
    sendMessage(mpiMessage);
  }

  private void sendMessage(MPIMessage msgObj1) {
    if (routing.getSendingIds() != null && routing.getSendingIds().size() > 0) {
      List<Integer> sendIds = routing.getSendingIds();
      for (int i : sendIds) {
        channel.sendMessage(i, msgObj1, stream);
        // increment the ref count
        msgObj1.incrementRefCount();
      }
    }
  }

  @Override
  public void onReceiveComplete(int id, MPIMessage message) {
    // try to send further
    sendMessage(message);
    // we received a message, we need to determine weather we need to forward to another node
    // and process
    if (formatter != null) {
      Object object = formatter.format(message);
      receiver.receive(object);
    }
  }

  @Override
  public void onSendComplete(int id, MPIMessage message) {

  }
}
