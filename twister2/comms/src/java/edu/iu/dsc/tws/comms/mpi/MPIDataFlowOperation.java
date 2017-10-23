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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KryoSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.Routing;

public abstract class MPIDataFlowOperation implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowOperation.class.getName());

  // the configuration
  protected Config config;
  // the task plan
  protected TaskPlan instancePlan;

  protected int edge;
  // the router that gives us the possible routes
  protected IRouter router;
  protected TWSMPIChannel channel;
  protected MessageReceiver receiver;
  protected MessageDeSerializer messageDeSerializer;
  protected MessageSerializer messageSerializer;
  // we may have multiple routes throughus
  protected Map<Integer, Routing> expectedRoutes;
  protected MessageReceiver partialReceiver;
  protected MessageType type;

  /**
   * The send sendBuffers used by the operation
   */
  protected Queue<MPIBuffer> sendBuffers;

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  protected Map<Integer, List<MPIBuffer>> receiveBuffers;

  /**
   * Pending send messages
   */
  protected Queue<Pair<Object, MPISendMessage>> pendingSendMessages;

  /**
   * Non grouped current messages
   */
  private Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected KryoSerializer kryoSerializer;

  public MPIDataFlowOperation(TWSMPIChannel channel) {
    this.channel = channel;
  }


  @Override
  public void init(Config cfg, MessageType messageType, TaskPlan plan, int graphEdge,
                   MessageReceiver rcvr, MessageReceiver partialRcvr) {
    this.config = cfg;
    this.instancePlan = plan;
    this.edge = graphEdge;
    this.receiver = rcvr;
    this.partialReceiver = partialRcvr;
    this.type = messageType;

    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

    this.sendBuffers = new ArrayBlockingQueue<MPIBuffer>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }
    this.receiveBuffers = new HashMap<>();

    router = setupRouting();
    this.expectedRoutes = router.expectedRoutes();

    // later look at how not to allocate pairs for this each time
    pendingSendMessages = new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
        MPIContext.sendPendingMax(config, 1024));

    // now setup the sends and receives
    setupCommunication();

    // initialize the serializers
    initSerializers();
  }

  protected void initSerializers() {
    kryoSerializer = new KryoSerializer();
    messageDeSerializer = new MPIMessageDeSerializer(kryoSerializer);
    messageSerializer = new MPIMessageSerializer(sendBuffers, kryoSerializer);
    // initialize the serializers
    messageSerializer.init(config, false);
    messageDeSerializer.init(config, false);
  }

  @Override
  public void finish() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void injectPartialResult(int source, Object message) {
    sendMessage(source, message);
  }

  @Override
  public void sendPartial(int source, Object message) {
    throw new NotImplementedException("Not implemented method");
  }

  protected abstract IRouter setupRouting();
  protected abstract void routeReceivedMessage(MessageHeader message, List<Integer> routes);
  protected abstract void routeSendMessage(int source, MessageHeader message, List<Integer> routes);
  protected abstract void sendCompleteMPIMessage(int source, MPIMessage message);

  /**
   * Sends a complete message
   * @param message the message object
   */
  @Override
  public boolean send(int source, Object message) {
    LOG.log(Level.INFO, "Sending message 1...");
    return sendMessage(source, message);
  }

  private boolean sendMessage(int source, Object message) {
    LOG.log(Level.INFO, "Sending message...");
    // first we need to create a message header
    // todo: figure out length and destination
    MessageHeader header = MessageHeader.newBuilder(source, 0, edge, 0, source).build();

    MPIMessage mpiMessage = new MPIMessage(source, type,
        header, 0, MPIMessageDirection.OUT, this);

    // create a send message to keep track of the serialization
    MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage);
    // this need to use the available buffers
    // we need to advertise the available buffers to the upper layers
    messageSerializer.build(message, sendMessage);

    // okay we could build fully
    if (sendMessage.serializedState() == MPISendMessage.SerializedState.FINISHED) {
      sendCompleteMPIMessage(source, mpiMessage);
      return true;
    } else {
      // now try to put this into pending
      return pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(message, sendMessage));
    }
  }

  @Override
  public void progress() {
    while (pendingSendMessages.size() > 0) {
      // take out pending messages
      Pair<Object, MPISendMessage> pair = pendingSendMessages.peek();
      MPISendMessage message = (MPISendMessage)
          messageSerializer.build(pair.getKey(), pair.getValue());

      // okay we build the message, send it
      if (message.serializedState() == MPISendMessage.SerializedState.FINISHED) {
        sendCompleteMPIMessage(message.getSource(), message.getMPIMessage());
        pendingSendMessages.remove();
      } else {
        break;
      }
    }
  }

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
      // register with the channel
      channel.receiveMessage(recv, edge, this, recvList);
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
    if (MPIMessageDirection.IN == message.getMessageDirection()) {
      List<MPIBuffer> list = receiveBuffers.get(id);
      for (MPIBuffer buffer : message.getBuffers()) {
        list.add(buffer);
      }
    } else if (MPIMessageDirection.OUT == message.getMessageDirection()) {
      Queue<MPIBuffer> queue = sendBuffers;
      for (MPIBuffer buffer : message.getBuffers()) {
        queue.offer(buffer);
      }
    }
  }

  @Override
  public void onReceiveComplete(int id, int e, MPIBuffer buffer) {
    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage currentMessage = currentMessages.get(id);
    if (currentMessage == null) {
      currentMessage = new MPIMessage(type, MPIMessageDirection.IN, this);
      currentMessages.put(id, currentMessage);
    }

    Object object = messageDeSerializer.buid(buffer, currentMessage, e);
    // if the message is complete, send it further down and call the receiver
    if (currentMessage.isComplete()) {

      // we may need to pass this down to others
      passMessageDownstream(currentMessage);
      // we received a message, we need to determine weather we need to
      // forward to another node and process
      // check weather this is a message for partial or final receiver
      receiver.onMessage(object);
      // okay we built this message, lets remove it from the map
      currentMessages.remove(id);
    }
  }

  /**
   * By default we are not doing anything here and the specific operations can override this
   *
   * @param currentMessage
   */
  protected void passMessageDownstream(MPIMessage currentMessage) {
  }

  protected void messageReceived(MPIMessage completeMessage) {
  }

  @Override
  public boolean send(int source, Object message, int path) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message, int path) {
    return false;
  }
}
