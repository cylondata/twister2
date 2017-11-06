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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

public abstract class MPIDataFlowOperation implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowOperation.class.getName());

  // the configuration
  protected Config config;
  // the task plan
  protected TaskPlan instancePlan;

  protected int edge;
  // the router that gives us the possible routes
  protected TWSMPIChannel channel;
  protected MessageReceiver finalReceiver;
  protected MessageDeSerializer messageDeSerializer;
  protected MessageSerializer messageSerializer;
  // we may have multiple routes throughus
  protected MessageReceiver partialReceiver;
  protected MessageType type;
  protected int executor;
  /**
   * The send sendBuffers used by the operation
   */
  protected Queue<MPIBuffer> sendBuffers;

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  protected Map<Integer, Queue<MPIBuffer>> receiveBuffers;

  /**
   * Pending send messages
   */
  protected Queue<Pair<Object, MPISendMessage>> pendingSendMessages;

  /**
   * Non grouped current messages
   */
  private Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected KryoSerializer kryoSerializer;

  /**
   * A lock to serialize access to the resources
   */
  protected final Lock lock = new ReentrantLock();

  public MPIDataFlowOperation(TWSMPIChannel channel) {
    this.channel = channel;
  }

  @Override
  public void init(Config cfg, MessageType messageType, TaskPlan plan, int graphEdge,
                   MessageReceiver rcvr, MessageReceiver partialRcvr) {
    this.config = cfg;
    this.instancePlan = plan;
    this.edge = graphEdge;
    this.finalReceiver = rcvr;
    this.partialReceiver = partialRcvr;
    this.type = messageType;
    this.executor = instancePlan.getThisExecutor();

    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

    this.sendBuffers = new ArrayBlockingQueue<MPIBuffer>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }
    this.receiveBuffers = new HashMap<>();

    // this should setup a router
    setupRouting();

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
    kryoSerializer.init(new HashMap<String, Object>());

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

  protected abstract void setupRouting();
  protected abstract void routeReceivedMessage(MessageHeader message, List<Integer> routes);
  protected abstract void routeSendMessage(int source, List<Integer> routes);

  /**
   * Return the list of receiving executors for this
   * @return
   */
  protected abstract Set<Integer> receivingExecutors();

  /**
   * Initialize the message receiver with tasks from which messages are expected
   * For each sub edge in graph, for each path, gives the expected task ids
   *
   * path -> (ids)
   *
   * @return  expected task ids
   */
  protected abstract Map<Integer, List<Integer>> receiveExpectedTaskIds();

  /**
   * Default implementation returns 0 and specific implementations should override
   * @return
   */
  protected int destinationIdentifier() {
    return 0;
  }

  /**
   * Is this the final task
   * @return
   */
  protected abstract boolean isLast(int taskIdentifier);

  /**
   * Sends a complete message
   * @param message the message object
   */
  @Override
  public boolean send(int source, Object message) {
    return sendMessage(source, message);
  }

  private boolean sendMessage(int source, Object message) {
//    LOG.log(Level.INFO, "lock 00 " + executor);
    lock.lock();
    try {
      List<Integer> routes = new ArrayList<>();
      routeSendMessage(source, routes);
      Set<Integer> thisExecutorTasks = instancePlan.getChannelsOfExecutor(
          instancePlan.getThisExecutor());

      // now check if these routes are in this executor
      List<Integer> routesInThisExecutor = new ArrayList<>();


      // check weather some of the routes are in the same executor

      // LOG.log(Level.INFO, "Sending message of type: " + type);
      // this is a originating message. we are going to put ref count to 0 for now and
      // increment it later
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);

      // create a send message to keep track of the serialization
      // at the intial stage the sub-edge is 0
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          destinationIdentifier());
      // this need to use the available buffers
      // we need to advertise the available buffers to the upper layers
      messageSerializer.build(message, sendMessage);

      // okay we could build fully
      if (sendMessage.serializedState() == MPISendMessage.SerializedState.FINISHED) {
        // lets increment the reference count of this message
        mpiMessage.incrementRefCount();
        sendMessage(mpiMessage, routes);
        return true;
      } else {
        // now try to put this into pending
        return pendingSendMessages.offer(
            new ImmutablePair<Object, MPISendMessage>(message, sendMessage));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void progress() {
//    LOG.log(Level.INFO, "lock 11 " + executor);
    lock.lock();
    try {
      while (pendingSendMessages.size() > 0) {
        // take out pending messages
        Pair<Object, MPISendMessage> pair = pendingSendMessages.peek();
        MPISendMessage message = (MPISendMessage)
            messageSerializer.build(pair.getKey(), pair.getValue());

        // okay we build the message, send it
        if (message.serializedState() == MPISendMessage.SerializedState.FINISHED) {

          List<Integer> routes = new ArrayList<>();
          routeSendMessage(message.getSource(), routes);
          sendMessage(message.getMPIMessage(), routes);

          pendingSendMessages.remove();
        } else {
          break;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Setup the receives and send sendBuffers
   */
  protected void setupCommunication() {
    // we will receive from these
    int maxReceiveBuffers = MPIContext.receiveBufferCount(config);
    int receiveBufferSize = MPIContext.bufferSize(config);
    for (Integer recv : receivingExecutors()) {
      Queue<MPIBuffer> recvList = new LinkedList<>();
      for (int i = 0; i < maxReceiveBuffers; i++) {
        recvList.add(new MPIBuffer(receiveBufferSize));
      }
      // register with the channel
      LOG.info(instancePlan.getThisExecutor() + " Register to receive from: " + recv);
      channel.receiveMessage(recv, edge, this, recvList);
      receiveBuffers.put(recv, recvList);
    }

    // initialize the receive
    if (this.partialReceiver != null) {
      partialReceiver.init(receiveExpectedTaskIds());
    } else {
      this.finalReceiver.init(receiveExpectedTaskIds());
    }

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
        // we need to convert the send id to a MPI process id
        int e = instancePlan.getExecutorForChannel(i);
        channel.sendMessage(e, msgObj1, this);
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
//    LOG.log(Level.INFO, "lock 22 " + executor);
    lock.lock();
    try {
      if (MPIMessageDirection.IN == message.getMessageDirection()) {
        Queue<MPIBuffer> list = receiveBuffers.get(id);
        for (MPIBuffer buffer : message.getBuffers()) {
          // we need to reset the buffer so it can be used again
          buffer.getByteBuffer().clear();
//        LOG.info("Releasing receive buffer to: " + id);
          list.offer(buffer);
        }
      } else if (MPIMessageDirection.OUT == message.getMessageDirection()) {
        Queue<MPIBuffer> queue = sendBuffers;
        for (MPIBuffer buffer : message.getBuffers()) {
//        LOG.info("Releasing send buffer");
          // we need to reset the buffer so it can be used again
          buffer.getByteBuffer().clear();
          queue.offer(buffer);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onReceiveComplete(int id, int e, MPIBuffer buffer) {
//    LOG.log(Level.INFO, "lock 33 " + executor);
    lock.lock();
    try {
      // we need to try to build the message here, we may need many more messages to complete
      MPIMessage currentMessage = currentMessages.get(id);
      if (currentMessage == null) {
        currentMessage = new MPIMessage(type, MPIMessageDirection.IN, this);
        currentMessages.put(id, currentMessage);
      }

      Object object = messageDeSerializer.buid(buffer, currentMessage, e);
      // if the message is complete, send it further down and call the receiver
      if (currentMessage.isComplete()) {
//      LOG.info("On receive complete message built");
        // we need to increment the ref count here before someone send it to another place
        // other wise they may free it before we do
        currentMessage.incrementRefCount();
        // we may need to pass this down to others
        passMessageDownstream(currentMessage);
        // we received a message, we need to determine weather we need to
        // forward to another node and process
        // check weather this is a message for partial or final receiver
        receiveMessage(currentMessage, object);
        // okay we built this message, lets remove it from the map
        currentMessages.remove(id);
        // okay lets try to free the buffers of this message
        currentMessage.release();
      } else {
        LOG.info("On receive complete message NOT built");
      }
    } finally {
      lock.unlock();
    }
  }

  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
    // check weather this message is for a sub task
    if (!isLast(messageDestId) && partialReceiver != null) {
      partialReceiver.onMessage(header, object);
    } else {
      finalReceiver.onMessage(header, object);
    }
  }

  /**
   * By default we are not doing anything here and the specific operations can override this
   *
   * @param currentMessage
   */
  protected void passMessageDownstream(MPIMessage currentMessage) {
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
