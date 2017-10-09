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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.Routing;

public abstract class MPIDataFlowOperation implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  protected Config config;
  protected TaskPlan instancePlan;
  protected Set<Integer> sources;
  protected Set<Integer> destinations;
  protected int stream;
  protected IRouter router;
  protected TWSMPIChannel channel;
  protected MessageReceiver receiver;
  protected MessageDeSerializer messageDeSerializer;
  protected MessageSerializer messageSerializer;
  protected int thisTask;
  protected Map<Integer, Routing> expectedRoutes;
  protected MessageReceiver partialReceiver;

  /**
   * The send sendBuffers used by the operation
   */
  protected Queue<MPIBuffer> sendBuffers = new LinkedList<>();

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  protected Map<Integer, List<MPIBuffer>> receiveBuffers = new HashMap<>();

  public MPIDataFlowOperation(TWSMPIChannel channel) {
    this.channel = channel;
  }

  @Override
  public void init(Config cfg, int task, TaskPlan plan, Set<Integer> srcs,
                   Set<Integer> dests, int messageStream, MessageReceiver rcvr,
                   MessageDeSerializer fmtr, MessageSerializer bldr,
                   MessageReceiver partialRcvr) {
    this.config = cfg;
    this.instancePlan = plan;
    this.sources = srcs;
    this.destinations = dests;
    this.stream = messageStream;
    this.messageDeSerializer = fmtr;
    this.messageSerializer = bldr;
    this.receiver = rcvr;
    this.sendBuffers = new LinkedList<>();
    this.thisTask = task;
    this.partialReceiver = partialRcvr;


    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }

    router = setupRouting();
    this.expectedRoutes = router.expectedRoutes();

    // now setup the sends and receives
    setupCommunication();
  }

  @Override
  public void injectPartialResult(Message message) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void sendPartialMessage(Message message) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void finish() {
    throw new RuntimeException("Not implemented");
  }

  protected abstract IRouter setupRouting();

  protected abstract void routeMessage(MessageHeader message, List<Integer> routes);
  /**
   * Setup the receives and send sendBuffers
   */
  protected void setupCommunication() {
    Set<Integer> receiving = new HashSet<>();
    Map<Integer, List<Integer>> receiveMap = new HashMap<>();
    // we will receive from these
    for (Map.Entry<Integer, Routing> e : expectedRoutes.entrySet()) {
      receiving.addAll(e.getValue().getUpstreamIds());
      receiveMap.put(e.getKey(), e.getValue().getUpstreamIds());
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

    // initialize the receive
    this.receiver.init(receiveMap);

    // configure the send sendBuffers
    int sendBufferSize = MPIContext.bufferSize(config);
    int sendBufferCount = MPIContext.sendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      MPIBuffer buffer = new MPIBuffer(sendBufferSize);
      sendBuffers.offer(buffer);
    }
  }

  protected void sendMessage(MPIMessage msgObj1, List<Integer> sendIds) {
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
  public void release(MPIMessage message) {
    if (message.doneProcessing()) {
      releaseTheBuffers(message.getOriginatingId(), message);
    }
  }

  @Override
  public void onSendComplete(int id, int messageStream, MPIMessage message) {
    // ok we don't have anything else to do
    message.release();
  }

  @Override
  public void close() {
  }

  protected void releaseTheBuffers(int id, MPIMessage message) {
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

  protected MessageHeader buildHeader(MPIBuffer buffer) {
    int sourceId = buffer.getByteBuffer().getInt();
    int destId = buffer.getByteBuffer().getInt();
    int edge = buffer.getByteBuffer().getInt();
    int length = buffer.getByteBuffer().getInt();
    int lastNode = buffer.getByteBuffer().getInt();

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, destId, edge, length, lastNode);
    // first build the header
    return headerBuilder.build();
  }
}
