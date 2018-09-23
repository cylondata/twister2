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
package edu.iu.dsc.tws.comms.dfw;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;

public class ChannelDataFlowOperation implements ChannelListener, ChannelMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(ChannelDataFlowOperation.class.getName());

  // the configuration
  private Config config;
  // the task plan
  private TaskPlan instancePlan;

  /**
   * The edge used
   */
  private int edge;
  /**
   * The network channel
   */
  private TWSChannel channel;
  /**
   * Set of de-serializers
   */
  private Map<Integer, MessageDeSerializer> messageDeSerializer;

  /**
   * Set of serializers
   */
  private Map<Integer, MessageSerializer> messageSerializer;

  // we may have multiple routes throughus
  private MessageType dataType;

  /**
   * The key type
   */
  private MessageType keyType = MessageType.BYTE;
  /**
   * Receive data type
   */
  private MessageType receiveDataType;
  /**
   * Receive key type
   */
  private MessageType receiveKeyType;
  /**
   * Weather keys are involved
   */
  private boolean isKeyed = false;
  /**
   * Lock for serializing the operations
   */
  private Lock lock = new ReentrantLock();

  /**
   * Executor id
   */
  private int executor;
  /**
   * The send sendBuffers used by the operation
   */
  private Queue<DataBuffer> sendBuffers;

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  private Map<Integer, Queue<DataBuffer>> receiveBuffers;

  /**
   * Local buffers that are used when receive buffers need to be freed. Buffer are only added
   * to the list when needed
   */
  private Queue<DataBuffer> localReceiveBuffers;

  /**
   * Pending send messages
   */
  private Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>>
      pendingSendMessagesPerSource;

  /**
   * Pending receives in case the receives are not ready
   */
  private Map<Integer, Queue<Pair<Object, ChannelMessage>>> pendingReceiveMessagesPerSource;

  /**
   * Pending deserializations
   */
  private Map<Integer, Queue<ChannelMessage>> pendingReceiveDeSerializations;
  /**
   * Non grouped current messages
   */
  private Map<Integer, ChannelMessage> currentMessages = new HashMap<>();

  /**
   * These are the workers from which we receive messages
   */
  private Set<Integer> receivingExecutors;

  /**
   * The message receiver for MPI messages
   */
  private ChannelReceiver receiver;

  /**
   * Send communicationProgress tracker
   */
  private ProgressionTracker sendProgressTracker;

  /**
   * Receive communicationProgress tracker
   */
  private ProgressionTracker receiveProgressTracker;

  /**
   * Deserialize communicationProgress tracke
   */
  private ProgressionTracker deserializeProgressTracker;

  private Map<Integer, AtomicBoolean> sendsDone = new HashMap<>();

  private Map<Integer, AtomicBoolean> receivesDone = new HashMap<>();

  private AtomicInteger externalSendsPending = new AtomicInteger(0);

  public ChannelDataFlowOperation(TWSChannel channel) {
    this.channel = channel;
  }


  /**
   * init method
   */
  public void init(Config cfg, MessageType messageType, MessageType rcvDataType,
                   MessageType kType, MessageType rcvKeyType, TaskPlan plan,
                   int graphEdge, Set<Integer> recvExecutors,
                   boolean lastReceiver, ChannelReceiver msgReceiver,
                   Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>>
                       pendingSendPerSource,
                   Map<Integer, Queue<Pair<Object, ChannelMessage>>> pRMPS,
                   Map<Integer, Queue<ChannelMessage>> pendingReceiveDesrialize,
                   Map<Integer, MessageSerializer> serializer,
                   Map<Integer, MessageDeSerializer> deSerializer, boolean keyed) {
    this.config = cfg;
    this.instancePlan = plan;
    this.edge = graphEdge;
    this.dataType = messageType;
    this.receiveDataType = rcvDataType;
    this.receiveKeyType = rcvKeyType;
    this.keyType = kType;
    this.executor = instancePlan.getThisExecutor();
    this.receivingExecutors = recvExecutors;
    this.receiver = msgReceiver;
    this.isKeyed = keyed;

    this.pendingReceiveMessagesPerSource = pRMPS;
    this.pendingSendMessagesPerSource = pendingSendPerSource;
    this.pendingReceiveDeSerializations = pendingReceiveDesrialize;

    this.messageSerializer = serializer;
    this.messageDeSerializer = deSerializer;

    int noOfSendBuffers = DataFlowContext.broadcastBufferCount(config);
    int sendBufferSize = DataFlowContext.bufferSize(config);

    this.sendBuffers = new ArrayBlockingQueue<DataBuffer>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new DataBuffer(channel.createBuffer(sendBufferSize)));
    }
    this.receiveBuffers = new HashMap<>();
    this.localReceiveBuffers = new ArrayDeque<DataBuffer>();

    LOG.log(Level.FINE, String.format("%d setup communication", instancePlan.getThisExecutor()));
    // now setup the sends and receives
    setupCommunication();

    // initialize the serializers
    LOG.fine(String.format("%d setup initializers", instancePlan.getThisExecutor()));
    initSerializers();

    initProgressTrackers();

    for (int sendSources : pendingSendMessagesPerSource.keySet()) {
      sendsDone.put(sendSources, new AtomicBoolean(false));
    }

    for (int receiveTasks : pendingReceiveMessagesPerSource.keySet()) {
      receivesDone.put(receiveTasks, new AtomicBoolean(false));
    }
  }

  /**
   * init method
   */
  public void init(Config cfg, MessageType messageType, TaskPlan plan,
                   int graphEdge, Set<Integer> recvExecutors,
                   boolean lastReceiver, ChannelReceiver msgReceiver,
                   Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>>
                       pendingSendPerSource,
                   Map<Integer, Queue<Pair<Object, ChannelMessage>>> pRMPS,
                   Map<Integer, Queue<ChannelMessage>> pendingReceiveDesrialize,
                   Map<Integer, MessageSerializer> serializer,
                   Map<Integer, MessageDeSerializer> deSerializer, boolean keyed) {
    init(cfg, messageType, messageType, keyType, keyType,
        plan, graphEdge, recvExecutors, lastReceiver, msgReceiver,
        pendingSendPerSource, pRMPS, pendingReceiveDesrialize, serializer, deSerializer, keyed);
  }

  private void initSerializers() {
    // initialize the serializers
    for (MessageSerializer serializer : messageSerializer.values()) {
      serializer.init(config, sendBuffers, isKeyed);
    }
    for (MessageDeSerializer deSerializer : messageDeSerializer.values()) {
      deSerializer.init(config, isKeyed);
    }
  }

  private void initProgressTrackers() {
    Set<Integer> sendItems = pendingSendMessagesPerSource.keySet();
    sendProgressTracker = new ProgressionTracker(sendItems);

    Set<Integer> receiveItems = pendingReceiveMessagesPerSource.keySet();
    receiveProgressTracker = new ProgressionTracker(receiveItems);

    Set<Integer> desrializeItems = pendingReceiveDeSerializations.keySet();
    deserializeProgressTracker = new ProgressionTracker(desrializeItems);
  }

  /**
   * Setup the receives and send sendBuffers
   */
  private void setupCommunication() {
    // we will receive from these
    int maxReceiveBuffers = DataFlowContext.receiveBufferCount(config);
    int receiveBufferSize = DataFlowContext.bufferSize(config);
    for (Integer recv : receivingExecutors) {
      Queue<DataBuffer> recvList = new LinkedBlockingQueue<>();
      for (int i = 0; i < maxReceiveBuffers; i++) {
        recvList.add(new DataBuffer(channel.createBuffer(receiveBufferSize)));
      }
      // register with the channel
      LOG.fine(instancePlan.getThisExecutor() + " Register to receive from: " + recv);
      channel.receiveMessage(recv, edge, this, recvList);
      receiveBuffers.put(recv, recvList);
    }

    // configure the send sendBuffers
    int sendBufferSize = DataFlowContext.bufferSize(config);
    int sendBufferCount = DataFlowContext.sendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      DataBuffer buffer = new DataBuffer(channel.createBuffer(sendBufferSize));
      sendBuffers.offer(buffer);
    }
  }

  /**
   * Sends a message from a partil location
   *
   * @param source source id
   * @param message the actual message
   * @param target an specific target
   * @param flags message flags
   * @param routingParameters routing parameter
   * @return true if the message is accepted
   */
  public boolean sendMessagePartial(int source, Object message, int target,
                                    int flags, RoutingParameters routingParameters) {
    // for partial sends we use minus value to find the correct queue
    ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(source * -1 - 1);
    return offerForSend(source, message, target, flags,
        routingParameters, pendingSendMessages);
  }

  /**
   * Sends a message from a originating location
   *
   * @param source source id
   * @param message the actual message
   * @param target an specific target
   * @param flags message flags
   * @param routingParameters routing parameter
   * @return true if the message is accepted
   */
  public boolean sendMessage(int source, Object message, int target,
                             int flags, RoutingParameters routingParameters) {
    ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(source);
    if (pendingSendMessages == null) {
      throw new RuntimeException(String.format("%d No send messages %d", executor, source));
    }
    return offerForSend(source, message, target, flags,
        routingParameters, pendingSendMessages);
  }

  @Override
  public void onReceiveComplete(int id, int e, DataBuffer buffer) {
    // we need to try to build the message here, we may need many more messages to complete
    ChannelMessage currentMessage = currentMessages.get(id);
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    byteBuffer.position(buffer.getSize());
    byteBuffer.flip();
    if (currentMessage == null) {
      currentMessage = new ChannelMessage(id, receiveDataType, MessageDirection.IN, this);
      if (isKeyed) {
        currentMessage.setKeyType(receiveKeyType);
      }
      currentMessages.put(id, currentMessage);
      MessageHeader header = messageDeSerializer.get(id).buildHeader(buffer, e);
      currentMessage.setHeader(header);
      currentMessage.setHeaderSize(16);
    }
    currentMessage.addBuffer(buffer);
    currentMessage.build();

    if (currentMessage.isComplete()) {
      currentMessages.remove(id);
      Queue<ChannelMessage> deserializeQueue = pendingReceiveDeSerializations.get(id);
      if (!deserializeQueue.offer(currentMessage)) {
        throw new RuntimeException(executor + " We should have enough space: "
            + deserializeQueue.size());
      }
    }
  }

  /**
   * Weather we have more data to complete
   */
  public boolean isComplete() {
    for (Map.Entry<Integer, Queue<Pair<Object, ChannelMessage>>> e
        : pendingReceiveMessagesPerSource.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    for (Map.Entry<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>> e
        : pendingSendMessagesPerSource.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    for (Map.Entry<Integer, Queue<ChannelMessage>> e : pendingReceiveDeSerializations.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    return externalSendsPending.get() == 0;
  }

  /**
   * Progress the serializations and receives, this method must be called by threads to
   * send messages through this communication
   */
  public void progress() {
    if (sendProgressTracker.canProgress()) {
      int sendId = sendProgressTracker.next();
      if (sendId != Integer.MIN_VALUE) {
        boolean done = sendProgress(pendingSendMessagesPerSource.get(sendId), sendId);
//        LOG.log(Level.INFO, String.format("SendID %d - %b", sendId, done));
        AtomicBoolean b = sendsDone.get(sendId);
        b.set(done);
        sendProgressTracker.finish(sendId);
      }
    }

    if (deserializeProgressTracker.canProgress()) {
      int deserializeId = deserializeProgressTracker.next();
      if (deserializeId != Integer.MIN_VALUE) {
        receiveDeserializeProgress(
            pendingReceiveDeSerializations.get(deserializeId).poll(), deserializeId);
        deserializeProgressTracker.finish(deserializeId);
      }
    }

    if (receiveProgressTracker.canProgress()) {
      int receiveId = receiveProgressTracker.next();
      if (receiveId != Integer.MIN_VALUE) {
        boolean done = receiveProgress(pendingReceiveMessagesPerSource.get(receiveId));
        AtomicBoolean b = receivesDone.get(receiveId);
        b.set(done);
        receiveProgressTracker.finish(receiveId);
      }
    }
  }

  private boolean offerForSend(int source, Object message, int target, int flags,
                               RoutingParameters routingParameters,
                               ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages) {
    if (pendingSendMessages.remainingCapacity() > 0) {
      ChannelMessage channelMessage = new ChannelMessage(source, dataType,
          MessageDirection.OUT, this);

      int path = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        path = routingParameters.getDestinationId();
      }
      OutMessage sendMessage = new OutMessage(source, channelMessage, edge,
          path, target, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      return pendingSendMessages.offer(
          new ImmutablePair<Object, OutMessage>(message, sendMessage));
    }
    return false;
  }

  private boolean sendProgress(Queue<Pair<Object, OutMessage>> pendingSendMessages, int sendId) {
    boolean canProgress = true;
    while (pendingSendMessages.size() > 0 && canProgress) {
      // take out pending messages
      Pair<Object, OutMessage> pair = pendingSendMessages.peek();
      OutMessage outMessage = pair.getValue();
      Object messageObject = pair.getKey();
      if (outMessage.serializedState() == OutMessage.SendState.INIT) {
        // send it internally
        int startOfInternalRouts = outMessage.getAcceptedInternalSends();
        List<Integer> inRoutes = new ArrayList<>(outMessage.getInternalSends());
        for (int i = startOfInternalRouts; i < outMessage.getInternalSends().size(); i++) {
          boolean receiveAccepted;
          lock.lock();
          try {
            receiveAccepted = receiver.receiveSendInternally(
                outMessage.getSource(), inRoutes.get(i), outMessage.getTarget(),
                outMessage.getFlags(), messageObject);
          } finally {
            lock.unlock();
          }
          if (!receiveAccepted) {
//            LOG.info(String.format("%d SendID %b", sendId, false));
            canProgress = false;
            break;
          }
          outMessage.incrementAcceptedInternalSends();
        }
        if (canProgress) {
          outMessage.setSendState(OutMessage.SendState.SENT_INTERNALLY);
        }
      }

      if (canProgress) {
        // we don't have an external executor to send this message
        if (outMessage.getExternalSends().size() == 0) {
          pendingSendMessages.poll();
          continue;
        }
        // at this point lets build the message
        //TODO: do we need to return the object since we are sending back the same outMessage
        //TODO: that we send in as a param. This can cause a confusion
        OutMessage message = (OutMessage)
            messageSerializer.get(sendId).build(pair.getKey(), outMessage);
        // okay we build the message, send it
        if (message.serializedState() == OutMessage.SendState.SERIALIZED) {
          List<Integer> exRoutes = new ArrayList<>(message.getExternalSends());
          int startOfExternalRouts = message.getAcceptedExternalSends();
          int noOfExternalSends = startOfExternalRouts;
          lock.lock();
          try {
            if (!message.isOutCountUpdated()) {
              message.getChannelMessage().incrementRefCount(
                  message.getExternalSends().size());
              message.setOutCountUpdated(true);
            }
            for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
              boolean sendAccepted = sendMessageToTarget(message.getChannelMessage(),
                  exRoutes.get(i));
              // if no longer accepts stop
              if (!sendAccepted) {
                canProgress = false;

                break;
              } else {
                noOfExternalSends = message.incrementAcceptedExternalSends();
                externalSendsPending.incrementAndGet();
              }
            }
          } finally {
            lock.unlock();
          }

          if (noOfExternalSends == exRoutes.size()) {
            // we are done
            message.setSendState(OutMessage.SendState.FINISHED);
            pendingSendMessages.poll();
          }
        } else if (message.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
          // If the message is partially serialized we will clone the message and send a clone
          // the original message will be kept so that the rest of the message can be serialized
          if (message.getChannelMessage().getBuffers().size() == 0) {
            break;
          }
          List<Integer> exRoutes = new ArrayList<>(message.getExternalSends());
          int startOfExternalRouts = message.getAcceptedExternalSends();

          //making a copy to send
          ChannelMessage sendCopy = createChannelMessageCopy(message.getChannelMessage());
          lock.lock();
          try {
            if (!message.isOutCountUpdated()) {
              sendCopy.incrementRefCount(
                  message.getExternalSends().size());
              message.setOutCountUpdated(true);
            }

            for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
              boolean sendAccepted = sendMessageToTarget(sendCopy, exRoutes.get(i));
              // if no longer accepts stop
              if (!sendAccepted) {
                canProgress = false;

                break;
              } else {
                externalSendsPending.incrementAndGet();
              }
            }
          } finally {
            message.setOutCountUpdated(false);
            lock.unlock();
          }
          //send and remove buffers from object
        } else {
          break;
        }
      }
    }
    return canProgress;
  }

  private ChannelMessage createChannelMessageCopy(ChannelMessage channelMessage) {
    ChannelMessage copy = new ChannelMessage();
    //Values that are not copied: refCount,
    copy.setMessageDirection(channelMessage.getMessageDirection());
    copy.setReleaseListener(channelMessage.getReleaseListener());
    copy.setOriginatingId(channelMessage.getOriginatingId());
    copy.setHeader(channelMessage.getHeader());
    copy.setComplete(channelMessage.isComplete());
    copy.setType(channelMessage.getType());
    copy.setKeyType(channelMessage.getKeyType());
    copy.setHeaderSize(channelMessage.getHeaderSize());
    copy.setReceivedState(channelMessage.getReceivedState());
    copy.addBuffers(channelMessage.getNormalBuffers());
    copy.addOverFlowBuffers(channelMessage.getOverflowBuffers());

    //remove the buffers from the original message
    channelMessage.removeAllBuffers();
    return copy;
  }

  private void receiveDeserializeProgress(ChannelMessage currentMessage, int receiveId) {
    if (currentMessage == null) {
      return;
    }

    int id = currentMessage.getOriginatingId();
    MessageHeader header = currentMessage.getHeader();
    Object object = DataFlowContext.EMPTY_OBJECT;
    if ((header.getFlags() & MessageFlags.END) != MessageFlags.END) {
      object = messageDeSerializer.get(receiveId).build(currentMessage,
          currentMessage.getHeader().getEdge());
    } else if ((header.getFlags() & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
      object = messageDeSerializer.get(receiveId).build(currentMessage,
          currentMessage.getHeader().getEdge());
    }
    Queue<Pair<Object, ChannelMessage>> pendingReceiveMessages =
        pendingReceiveMessagesPerSource.get(id);
    currentMessage.setReceivedState(ChannelMessage.ReceivedState.INIT);
    if (!pendingReceiveMessages.offer(new ImmutablePair<>(object, currentMessage))) {
      throw new RuntimeException(executor + " We should have enough space: "
          + pendingReceiveMessages.size());
    }
  }


  private boolean receiveProgress(Queue<Pair<Object, ChannelMessage>> pendingReceiveMessages) {
    boolean canProgress = true;
    while (pendingReceiveMessages.size() > 0) {
      Pair<Object, ChannelMessage> pair = pendingReceiveMessages.peek();
      ChannelMessage.ReceivedState state = pair.getRight().getReceivedState();
      ChannelMessage currentMessage = pair.getRight();
      Object object = pair.getLeft();

      if (state == ChannelMessage.ReceivedState.INIT) {
        currentMessage.incrementRefCount();
      }

      lock.lock();
      try {
        if (state == ChannelMessage.ReceivedState.DOWN
            || state == ChannelMessage.ReceivedState.INIT) {
          currentMessage.setReceivedState(ChannelMessage.ReceivedState.DOWN);
          if (!receiver.passMessageDownstream(object, currentMessage)) {
            canProgress = false;
            break;
          }
          currentMessage.setReceivedState(ChannelMessage.ReceivedState.RECEIVE);
          if (!receiver.receiveMessage(currentMessage, object)) {
            canProgress = false;
            break;
          }
          currentMessage.release();
          pendingReceiveMessages.poll();
        } else if (state == ChannelMessage.ReceivedState.RECEIVE) {
          currentMessage.setReceivedState(ChannelMessage.ReceivedState.RECEIVE);
          if (!receiver.receiveMessage(currentMessage, object)) {
            canProgress = false;
            break;
          }
          currentMessage.release();
          pendingReceiveMessages.poll();
        }
      } finally {
        lock.unlock();
      }
    }
    return canProgress;
  }

  private boolean sendMessageToTarget(ChannelMessage channelMessage, int i) {
//    channelMessage.incrementRefCount();
    int e = instancePlan.getExecutorForChannel(i);
    return channel.sendMessage(e, channelMessage, this);
  }

  @Override
  public void release(ChannelMessage message) {
    if (message.doneProcessing()) {
      int originatingId = message.getOriginatingId();
      releaseTheBuffers(originatingId, message);
    }
  }

  private int sendCount = 0;

  @Override
  public void onSendComplete(int id, int messageStream, ChannelMessage message) {
    // ok we don't have anything else to do
    message.release();
    externalSendsPending.getAndDecrement();
  }

  @Override
  public void freeReceiveBuffers(int id, int stream) {
    ChannelMessage currentMessage = currentMessages.get(id);
    if (currentMessage == null) {
      return;
    }
    if (currentMessage.getNormalBuffers().size() == 0) {
      LOG.info("There are no receive buffers to be released for rank : " + id);
      return;
    }
    //Need to reuse created byte[]'s
    DataBuffer local = null;
    int receiveBufferSize = DataFlowContext.bufferSize(config);
    if (localReceiveBuffers.size() == 0) {
      local = new DataBuffer(ByteBuffer.allocate(receiveBufferSize));
    } else {
      local = localReceiveBuffers.poll();
    }
    copyToLocalBuffer(id, currentMessage.getNormalBuffers().remove(0), local, currentMessage);
  }

  private void copyToLocalBuffer(int id, DataBuffer dataBuffer, DataBuffer localBuffer,
                                 ChannelMessage message) {
    ByteBuffer original = dataBuffer.getByteBuffer();
    ByteBuffer local = localBuffer.getByteBuffer();
    int position = original.position();
    original.rewind();
    local.put(original);
    local.flip();
    local.position(position);
    localBuffer.setSize(dataBuffer.getSize());
    message.addToOverFlowBuffer(localBuffer);
    original.clear();
    Queue<DataBuffer> list = receiveBuffers.get(id);
    if (!list.offer(dataBuffer)) {
      throw new RuntimeException(String.format("%d Buffer release failed for target %d",
          executor, message.getHeader().getDestinationIdentifier()));
    }
  }

  private void releaseTheBuffers(int id, ChannelMessage message) {
    if (MessageDirection.IN == message.getMessageDirection()) {
      Queue<DataBuffer> list = receiveBuffers.get(id);
      for (DataBuffer buffer : message.getNormalBuffers()) {
        // we need to reset the buffer so it can be used again
        buffer.getByteBuffer().clear();
        if (!list.offer(buffer)) {
          throw new RuntimeException(String.format("%d Buffer release failed for target %d",
              executor, message.getHeader().getDestinationIdentifier()));
        }
      }
      if (message.getOverflowBuffers().size() > 0) {
        for (DataBuffer byteBuffer : message.getOverflowBuffers()) {
          byteBuffer.getByteBuffer().clear();
          if (!localReceiveBuffers.offer(byteBuffer)) {
            throw new RuntimeException(String.format("%d Local buffer release failed for target %d",
                executor, message.getHeader().getDestinationIdentifier()));
          }
        }
        message.getOverflowBuffers().clear();
      }
    } else if (MessageDirection.OUT == message.getMessageDirection()) {
      ArrayBlockingQueue<DataBuffer> queue = (ArrayBlockingQueue<DataBuffer>) sendBuffers;
      for (DataBuffer buffer : message.getNormalBuffers()) {
        // we need to reset the buffer so it can be used again
        buffer.getByteBuffer().clear();
        if (!queue.offer(buffer)) {
          throw new RuntimeException(String.format("%d Buffer release failed for source %d %d %d",
              executor, message.getOriginatingId(), queue.size(), queue.remainingCapacity()));
        }
      }
    }
  }

  public TaskPlan getInstancePlan() {
    return instancePlan;
  }

  public Config getConfig() {
    return config;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }
}
