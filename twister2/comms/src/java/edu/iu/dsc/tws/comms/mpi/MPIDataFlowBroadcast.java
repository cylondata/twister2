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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageBuilder;
import edu.iu.dsc.tws.comms.api.MessageFormatter;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.routing.BinaryTree;
import edu.iu.dsc.tws.comms.routing.Routing;

public class MPIDataFlowBroadcast implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  private Config config;
  private TaskPlan instancePlan;
  private Set<Integer> sources;
  private Set<Integer> destinations;
  private int stream;
  private Map<Integer, Routing> routings;
  private TWSMPIChannel channel;
  private MessageReceiver receiver;
  private MessageFormatter formatter;
  private MessageBuilder builder;
  private int thisTask;

  /**
   * The send sendBuffers used by the operation
   */
  private Queue<MPIBuffer> sendBuffers = new LinkedList<>();

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  private Map<Integer, List<MPIBuffer>> receiveBuffers = new HashMap<>();

  @Override
  public void init(Config cfg, int thisTask, TaskPlan plan, Set<Integer> srcs,
                   Set<Integer> dests, int messageStream, MessageReceiver rcvr,
                   MessageFormatter fmtr, MessageBuilder bldr) {
    this.config = cfg;
    this.instancePlan = plan;
    this.sources = srcs;
    this.destinations = dests;
    this.stream = messageStream;
    this.formatter = fmtr;
    this.builder = bldr;
    this.receiver = rcvr;
    this.sendBuffers = new LinkedList<>();
    this.thisTask = thisTask;


    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }

    // lets create the routing needed
    BinaryTree tree = new BinaryTree();
    tree.init(config, thisTask, instancePlan, sources, destinations, stream);

    routings = tree.routing(MPIContext.distinctRoutes(config, sources.size()));

    // now setup the sends and receives
    setupCommunication();
  }

  /**
   * Setup the receives and send sendBuffers
   */
  private void setupCommunication() {
    Set<Integer> receiving = new HashSet<>();
    // we will receive from these
    for (Map.Entry<Integer, Routing> e : routings.entrySet()) {
      receiving.addAll(e.getValue().getUpstreamIds());
    }

    int maxReceiveBuffers = MPIContext.receiveBufferCount(config);
    int receiveBufferSize = MPIContext.bufferSize(config);
    for (Integer recv : receiving) {
      List<MPIBuffer> recvList = new ArrayList<>();
      for (int i = 0; i < maxReceiveBuffers; i++) {
        recvList.add(new MPIBuffer(receiveBufferSize));
      }
      channel.receiveMessage(recv, stream, this, recvList);
      receiveBuffers.put(recv, recvList);
    }

    // configure the send sendBuffers
    int sendBufferSize = MPIContext.bufferSize(config);
    int sendBufferCount = MPIContext.sendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      MPIBuffer buffer = new MPIBuffer(sendBufferSize);
      sendBuffers.offer(buffer);
    }
  }

  @Override
  public void sendPartial(Message message) {
    throw new UnsupportedOperationException("partial messages not supported by broadcast");
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException("partial messages not supported by broadcast");
  }

  /**
   * Get the send buffers available. We can use these to construct the messages,
   * we should return a list of byte buffers, not the actual MPI buffer
   *
   * @return qieie of buffers
   */
  public Queue<MPIBuffer> getSendBuffers() {
    return sendBuffers;
  }

  /**
   * Sends a complete message
   * @param message the message object
   */
  @Override
  public void sendComplete(Message message) {
    // this need to use the available buffers
    // we need to advertise the available buffers to the upper layers
    Object msgObj = builder.build(message);

    if (!(msgObj instanceof MPIMessage)) {
      throw new IllegalArgumentException("Expecting a message of MPIMessage type");
    }

    MPIMessage mpiMessage = (MPIMessage) msgObj;
    Routing routing = routings.get(thisTask);
    if (routing == null || routing.getDownstreamIds().size() == 0) {
      throw new RuntimeException("Failed to get downstream tasks");
    }
    sendMessage(mpiMessage, routing.getDownstreamIds());
  }

  private void sendMessage(MPIMessage msgObj1, List<Integer> sendIds) {
    if (sendIds != null && sendIds.size() > 0) {
      // we need to increment before sending, otherwise message can get released
      // before we send all
      msgObj1.incrementRefCount(sendIds.size());
      for (int i : sendIds) {
        channel.sendMessage(i, msgObj1, this);
      }
    }
  }

  @Override
  public void onReceiveComplete(int id, int messageStream, MPIBuffer buffer) {
    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage completeMessage = null;

    if (completeMessage != null) {
      // we will get the routing based on the originating id
      Routing routing = routings.get(completeMessage.getOriginatingId());
      // try to send further
      sendMessage(completeMessage, routing.getDownstreamIds());

      // we received a message, we need to determine weather we need to forward to another node
      // and process
      if (formatter != null) {
        Object object = formatter.format(completeMessage);
        receiver.receive(object);
      }
    }
  }

  @Override
  public void onSendComplete(int id, int messageStream, MPIMessage message) {
    // ok we don't have anything else to do
    message.release();
  }

  private void releaseTheBuffers(int id, MPIMessage message) {
    if (MPIMessageType.RECEIVE == message.getMessageType()) {
      List<MPIBuffer> list = receiveBuffers.get(id);
      for (MPIBuffer buffer : message.getBuffers()) {
        list.add(buffer);
      }
    } else if (MPIMessageType.SEND == message.getMessageType()) {
      Queue<MPIBuffer> queue = sendBuffers;
      for (MPIBuffer buffer : message.getBuffers()) {
        queue.offer(buffer);
      }
    }
  }

  @Override
  public void release(MPIMessage message) {
    if (message.doneProcessing()) {
      releaseTheBuffers(message.getOriginatingId(), message);
    }
  }
}

