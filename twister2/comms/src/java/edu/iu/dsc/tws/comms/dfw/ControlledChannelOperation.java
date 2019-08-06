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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.channel.ChannelListener;
import edu.iu.dsc.tws.api.comms.channel.ChannelReceiver;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessageReleaseCallback;
import edu.iu.dsc.tws.api.comms.messaging.MessageDirection;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.MessageDeSerializer;
import edu.iu.dsc.tws.api.comms.packing.MessageSerializer;
import edu.iu.dsc.tws.api.config.Config;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class ControlledChannelOperation implements ChannelListener, ChannelMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(ControlledChannelOperation.class.getName());

  /**
   * The default path to be used
   */
  private static final int DEFAULT_PATH = -1;

  // the configuration
  private Config config;
  // the task plan
  private LogicalPlan instancePlan;

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
  private MessageType keyType = MessageTypes.BYTE;
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
   * Pending send messages
   */
  private Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendMessagesPerSource;

  /**
   * Pending receives in case the receives are not ready
   */
  private Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource;

  /**
   * Pending deserialization
   */
  private Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations;

  /**
   * Non grouped current messages, workerId, source, inMessage
   */
  private Map<Integer, InMessage> currentMessages = new HashMap<>();

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
   * Deserialize communicationProgress track
   */
  private ControlledProgressTracker receiveProgressTracker;

  /**
   * Number of external sends pending
   */
  private AtomicInteger externalSendsPending = new AtomicInteger(0);

  /**
   * The receive buffers that are not used
   */
  private Queue<DataBuffer> freeReceiveBuffers;

  /**
   * The receive task id groups
   */
  private List<IntArrayList> receiveIdGroups;

  /**
   * The sending groups
   */
  private List<IntArrayList> sendingGroupsTargets;

  /**
   * The sending groups
   */
  private List<IntArrayList> receiveGroupsSources;

  /**
   * Number of targets for each worker in the group
   */
  private Map<Integer, Integer> expectedReceivePerWorker;

  /**
   * The current receives for each worker in the current group
   */
  private Map<Integer, Integer> currentReceives = new HashMap<>();

  /**
   * Create the channel operation
   * @param channel
   * @param cfg
   * @param messageType
   * @param rcvDataType
   * @param kType
   * @param rcvKeyType
   * @param plan
   * @param graphEdge
   * @param recvExecutors
   * @param msgReceiver
   * @param pendingSendPerSource
   * @param pRMPS
   * @param pendingReceiveDesrialize
   * @param serializer
   * @param deSerializer
   * @param keyed
   * @param sendingGrpTargets
   * @param receiveGrpsSources
   */
  ControlledChannelOperation(TWSChannel channel, Config cfg,
                             MessageType messageType, MessageType rcvDataType,
                             MessageType kType, MessageType rcvKeyType, LogicalPlan plan,
                             int graphEdge, Set<Integer> recvExecutors,
                             ChannelReceiver msgReceiver,
                             Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendPerSource,
                             Map<Integer, Queue<InMessage>> pRMPS,
                             Map<Integer, Queue<InMessage>> pendingReceiveDesrialize,
                             Map<Integer, MessageSerializer> serializer,
                             Map<Integer, MessageDeSerializer> deSerializer, boolean keyed,
                             List<IntArrayList> sendingGrpTargets,
                             List<IntArrayList> receiveGrpsSources) {
    this.channel = channel;
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

    this.sendingGroupsTargets = sendingGrpTargets;
    this.receiveGroupsSources = receiveGrpsSources;

    int noOfSendBuffers = DataFlowContext.sendBuffersCount(config);
    int sendBufferSize = DataFlowContext.bufferSize(config);

    this.sendBuffers = new ArrayBlockingQueue<>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new DataBuffer(channel.createBuffer(sendBufferSize)));
    }
    this.receiveBuffers = new HashMap<>();

    LOG.log(Level.FINE, String.format("%d setup communication", instancePlan.getThisExecutor()));
    // now setup the sends and receives
    setupCommunication();

    // initialize the serializers
    LOG.fine(String.format("%d setup initializers", instancePlan.getThisExecutor()));
    initSerializers();

    setupReceiveGroups(receiveGroupsSources);

    initProgressTrackers();
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

    receiveProgressTracker = new ControlledProgressTracker(receiveGroupsSources);
  }

  /**
   * Setup the receives and send sendBuffers
   */
  private void setupCommunication() {
    // we will receive from these
    for (Integer recv : receivingExecutors) {
      Queue<DataBuffer> recvList = new LinkedBlockingQueue<>();
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
   * Start receiving from the next set of ids
   */
  public void setupReceiveGroups(List<IntArrayList> receivingIds) {
    this.receiveIdGroups = receivingIds;
    int max = Integer.MIN_VALUE;
    // first lets validate
    for (int i = 0; i < receivingIds.size(); i++) {
      List<Integer> group = receivingIds.get(i);
      if (group.size() > max) {
        max = group.size();
      }
    }
    // we put max group size equal buffers
    int receiveBufferSize = DataFlowContext.bufferSize(config);
    this.freeReceiveBuffers = new ArrayBlockingQueue<>(max);
    for (int i = 0; i < max; i++) {
      ByteBuffer byteBuffer = channel.createBuffer(receiveBufferSize);
      this.freeReceiveBuffers.offer(new DataBuffer(byteBuffer));
    }
  }

  /**
   * Start receiving from the next receiveGroup
   * @param receiveGroup the next receiveGroup
   */
  public void startGroup(int receiveGroup, int sendGroup,
                         Map<Integer, Integer> expectedReceives) {
    receiveProgressTracker.switchGroup(receiveGroup);
    List<Integer> receiveExecs = receiveIdGroups.get(receiveGroup);
    this.expectedReceivePerWorker = expectedReceives;
    currentReceives.clear();
    for (int i = 0; i < receiveExecs.size(); i++) {
      int exec = receiveExecs.get(i);
      int expected = expectedReceives.get(exec);
      // we offer the buffer only if we are expecting some messages
      if (expected > 0) {
        Queue<DataBuffer> list = receiveBuffers.get(exec);
        // poll the free receive buffers and ad to the receive
        DataBuffer buffer = freeReceiveBuffers.poll();
        if (buffer == null) {
          throw new RuntimeException("Buffer cannot be null");
        }
        list.offer(buffer);
      }
      currentReceives.put(exec, 0);
    }
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
    ArrayBlockingQueue<OutMessage> pendingSendMessages = pendingSendMessagesPerSource.get(source);
    if (pendingSendMessages == null) {
      throw new RuntimeException(String.format("%d No send messages %d", executor, source));
    }
    return offerForSend(source, message, target, flags,
        routingParameters, pendingSendMessages);
  }

  @Override
  public void onReceiveComplete(int id, int e, DataBuffer buffer) {
    // we need to try to build the message here, we may need many more messages to complete
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    byteBuffer.position(buffer.getSize());
    byteBuffer.flip();

    // we have the source of the message at 0th position as an integer
    int source = byteBuffer.getInt(0);
    InMessage currentMessage = currentMessages.get(source);
    if (currentMessage == null) {
      MessageHeader header = messageDeSerializer.get(source).buildHeader(buffer, e);

      MessageType recvDType = receiveDataType;
      MessageType recvKType = receiveKeyType;

      if ((header.getFlags() & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
        recvDType = MessageTypes.BYTE_ARRAY;
        recvKType = MessageTypes.EMPTY;
      }

      currentMessage = new InMessage(id, recvDType, this, header);
      if (isKeyed) {
        currentMessage.setKeyType(recvKType);
      }
      if (!currentMessage.addBufferAndCalculate(buffer)) {
        currentMessages.put(source, currentMessage);
      }
      // we add the message immediately to the deserialization as we can deserialize partially
      Queue<InMessage> deserializeQueue = pendingReceiveDeSerializations.get(source);
      if (!deserializeQueue.offer(currentMessage)) {
        throw new RuntimeException(executor + " We should have enough space: "
            + deserializeQueue.size());
      }
    } else {
      if (currentMessage.addBufferAndCalculate(buffer)) {
        currentMessages.remove(source);
      }
    }
  }

  /**
   * Weather we have more data to complete
   */
  public boolean isComplete() {
    for (Map.Entry<Integer, Queue<InMessage>> e
        : pendingReceiveMessagesPerSource.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    for (Map.Entry<Integer, ArrayBlockingQueue<OutMessage>> e
        : pendingSendMessagesPerSource.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    for (Map.Entry<Integer, Queue<InMessage>> e : pendingReceiveDeSerializations.entrySet()) {
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
        sendProgress(sendId);
        sendProgressTracker.finish(sendId);
      }
    }

    if (receiveProgressTracker.canProgress()) {
      int deserializeId = receiveProgressTracker.next();
      if (deserializeId != Integer.MIN_VALUE) {
        receiveDeserializeProgress(deserializeId);
        receiveProgress(deserializeId);
        receiveProgressTracker.finish(deserializeId);
      }
    }
  }

  /**
   * Put the message into internal queues, to be serialized and then send to the network channel
   *
   * @param source source
   * @param message data
   * @param target target
   * @param flags flags
   * @param routingParameters routing parameters
   * @param pendingSendMessages the message queue
   * @return true if message is accepted
   */
  private boolean offerForSend(int source, Object message, int target, int flags,
                               RoutingParameters routingParameters,
                               ArrayBlockingQueue<OutMessage> pendingSendMessages) {
    if (pendingSendMessages.remainingCapacity() > 0) {
      int path = DEFAULT_PATH;
      if (routingParameters.getExternalRoutes().size() > 0) {
        path = routingParameters.getDestinationId();
      }

      OutMessage sendMessage = new OutMessage(source, edge,
          path, target, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes(), dataType, keyType, this, message);

      // now try to put this into pending
      return pendingSendMessages.offer(sendMessage);
    }
    return false;
  }

  /**
   * Go through the out messages, create channel messages by using the serializer send them
   *
   * @param sendId send target
   */
  public void sendProgress(int sendId) {
    boolean canProgress = true;
    Queue<OutMessage> pendingSendMessages = pendingSendMessagesPerSource.get(sendId);
    while (pendingSendMessages.size() > 0 && canProgress) {
      // take out pending messages
      OutMessage outMessage = pendingSendMessages.peek();
      Object data = outMessage.getData();

      // first lets send the message to internal destinations
      canProgress = sendInternally(outMessage, data);

      if (canProgress) {
        // we don't have an external executor to send this message
        if (outMessage.getExternalSends().size() == 0) {
          pendingSendMessages.poll();
          continue;
        }
        Queue<ChannelMessage> channelMessages = outMessage.getChannelMessages();
        // at this point lets build the message
        ChannelMessage serializeMessage = (ChannelMessage)
            messageSerializer.get(sendId).build(outMessage.getData(), outMessage);
        if (serializeMessage != null) {
          // we are incrementing the reference count here
          channelMessages.offer(serializeMessage);
        }

        ChannelMessage chMessage = channelMessages.peek();
        if (chMessage == null) {
          break;
        }

        List<Integer> externalRoutes = outMessage.getExternalSends();
        // okay we build the message, send it
        if (outMessage.getSendState() == OutMessage.SendState.SERIALIZED) {
          int startOfExternalRouts = chMessage.getAcceptedExternalSends();
          canProgress = sendExternally(outMessage, chMessage, externalRoutes, startOfExternalRouts);
          if (chMessage.getAcceptedExternalSends() == externalRoutes.size()) {
            // we are done
            pendingSendMessages.poll();
            channelMessages.poll();
            // the send is completed, we need to notify
            receiver.sendCompleted(outMessage);
          }
        } else if (outMessage.getSendState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
          int startOfExternalRouts = chMessage.getAcceptedExternalSends();

          canProgress = sendExternally(outMessage, chMessage, externalRoutes, startOfExternalRouts);
          if (chMessage.getAcceptedExternalSends() == externalRoutes.size()) {
            // we are done sending this channel message
            channelMessages.poll();
          }
        } else {
          break;
        }
      }
    }
  }

  private boolean sendExternally(OutMessage outMessage, ChannelMessage chMessage,
                                 List<Integer> exRoutes, int startOfExternalRouts) {
    boolean canProgress = true;
    lock.lock();
    try {
      if (!chMessage.isOutCountUpdated()) {
        chMessage.incrementRefCount(outMessage.getExternalSends().size());
        chMessage.setOutCountUpdated(true);
      }
      for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
        boolean sendAccepted = sendMessageToTarget(chMessage, exRoutes.get(i));
        // if no longer accepts stop
        if (!sendAccepted) {
          canProgress = false;
          break;
        } else {
          //remove the buffers from the original message
          chMessage.incrementAcceptedExternalSends();
          externalSendsPending.incrementAndGet();
        }
      }
    } finally {
      lock.unlock();
    }
    return canProgress;
  }

  private boolean sendInternally(OutMessage outMessage, Object messageObject) {
    boolean canProgress = true;
    if (outMessage.getSendState() == OutMessage.SendState.INIT) {
      // send it internally
      int startOfInternalRouts = outMessage.getAcceptedInternalSends();
      List<Integer> inRoutes = outMessage.getInternalSends();
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
          canProgress = false;
          break;
        } else {
          receiver.sendCompleted(outMessage);
        }
        outMessage.incrementAcceptedInternalSends();
      }
      if (canProgress) {
        outMessage.setSendState(OutMessage.SendState.SENT_INTERNALLY);
      }
    }
    return canProgress;
  }

  /**
   * Progress deserialize
   * @param receiveId
   */
  public void receiveDeserializeProgress(int receiveId) {
    Queue<InMessage> msgQueue = pendingReceiveDeSerializations.get(receiveId);
    InMessage currentMessage = msgQueue.peek();
    if (currentMessage == null) {
      return;
    }

    if (currentMessage.getReceivedState() == InMessage.ReceivedState.INIT
        || currentMessage.getReceivedState() == InMessage.ReceivedState.BUILDING) {

      if (currentMessage.getReceivedState() == InMessage.ReceivedState.INIT) {
        Queue<InMessage> pendingReceiveMessages =
            pendingReceiveMessagesPerSource.get(currentMessage.getHeader().getSourceId());
        if (!pendingReceiveMessages.offer(currentMessage)) {
          throw new RuntimeException(executor + " We should have enough space: "
              + pendingReceiveMessages.size());
        }
        currentMessage.setReceivedState(InMessage.ReceivedState.BUILDING);
      }

      messageDeSerializer.get(receiveId).build(currentMessage,
          currentMessage.getHeader().getEdge());

      // lets check weather we have read everythong
      int readObjectNumber = currentMessage.getUnPkNumberObjects();
      // we need to get number of tuples and get abs because we are using -1 for single messages
      if (readObjectNumber == Math.abs(currentMessage.getHeader().getNumberTuples())) {
        currentMessage.setReceivedState(InMessage.ReceivedState.BUILT);
      }
    }

    // we remove only when the unpacking is complete and ready to receive
    if (currentMessage.getReceivedState() == InMessage.ReceivedState.BUILT
        || currentMessage.getReceivedState() == InMessage.ReceivedState.RECEIVE
        || currentMessage.getReceivedState() == InMessage.ReceivedState.DONE) {
      msgQueue.poll();
    }
  }

  /**
   * Progress the receive
   *
   * @param receiveId
   */
  public void receiveProgress(int receiveId) {
    Queue<InMessage> pendingReceiveMessages = pendingReceiveMessagesPerSource.get(receiveId);
    boolean canProgress = true;
    while (pendingReceiveMessages.size() > 0 && canProgress) {
      InMessage currentMessage = pendingReceiveMessages.peek();

      lock.lock();
      try {
        // lets keep track that we have completed a receive from this executor
        int workerId = currentMessage.getOriginatingId();
        int count = currentReceives.get(workerId);
        int expected = expectedReceivePerWorker.get(workerId);
        InMessage.ReceivedState receivedState = currentMessage.getReceivedState();

        if (receivedState == InMessage.ReceivedState.BUILT) {
          // if this message is built, we need to check how many we are expecting
          count++;
          currentReceives.put(workerId, count);
        }

        if (receivedState == InMessage.ReceivedState.BUILDING
            || receivedState == InMessage.ReceivedState.BUILT) {

          while (currentMessage.getBuiltMessages().size() > 0) {
            // get the first channel message
            ChannelMessage msg = currentMessage.getBuiltMessages().peek();
            if (msg != null) {
              if (!receiver.handleReceivedChannelMessage(msg)) {
                canProgress = false;
                break;
              }
              ChannelMessage releaseMsg = currentMessage.getBuiltMessages().poll();
              Objects.requireNonNull(releaseMsg).release();

              if (receivedState == InMessage.ReceivedState.BUILDING) {
                DataBuffer buffer = freeReceiveBuffers.poll();
                Queue<DataBuffer> list = receiveBuffers.get(workerId);
                if (buffer == null) {
                  throw new RuntimeException("Free buffers doesn't have any buffer");
                }
                // we get a free buffer and offer
                list.offer(buffer);
              } else {
                if (expected > count) {
                  DataBuffer buffer = freeReceiveBuffers.poll();
                  Queue<DataBuffer> list = receiveBuffers.get(workerId);
                  if (buffer == null) {
                    throw new RuntimeException("Free buffers doesn't have any buffer");
                  }
                  // we get a free buffer and offer
                  list.offer(buffer);
                }
              }
            }
          }

          if (receivedState == InMessage.ReceivedState.BUILT
              && currentMessage.getBuiltMessages().size() == 0 && canProgress) {
            currentMessage.setReceivedState(InMessage.ReceivedState.RECEIVE);
          }
        }

        if (receivedState == InMessage.ReceivedState.RECEIVE) {
          Object object = currentMessage.getDeserializedData();
          if (!receiver.receiveMessage(currentMessage.getHeader(), object)) {
            break;
          }

          currentMessage.setReceivedState(InMessage.ReceivedState.DONE);
          pendingReceiveMessages.poll();
        } else {
          break;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private boolean sendMessageToTarget(ChannelMessage channelMessage, int i) {
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

  @Override
  public void onSendComplete(int id, int messageStream, ChannelMessage message) {
    // ok we don't have anything else to do
    message.release();
    externalSendsPending.getAndDecrement();
  }

  private int addedFreedBuffers = 0;

  private void releaseTheBuffers(int id, ChannelMessage message) {
    if (MessageDirection.IN == message.getMessageDirection()) {
      // if we have received the full message we can release the buffer to free buffers,
      // otherwise we need to release to receive more
      for (DataBuffer buffer : message.getNormalBuffers()) {
        // we need to reset the buffer so it can be used again
        buffer.getByteBuffer().clear();
        addedFreedBuffers++;
        if (!freeReceiveBuffers.offer(buffer)) {
          throw new RuntimeException(String.format("%d Buffer release failed for target %d",
              executor, message.getHeader().getDestinationIdentifier()));
        }
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

  public LogicalPlan getInstancePlan() {
    return instancePlan;
  }

  public Config getConfig() {
    return config;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public void close() {
    for (int exec : receivingExecutors) {
      channel.releaseBuffers(exec, edge);
    }
  }
}
