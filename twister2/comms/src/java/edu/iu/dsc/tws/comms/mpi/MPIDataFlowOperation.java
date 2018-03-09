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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.CompletionListener;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KeyedContent;
import edu.iu.dsc.tws.comms.mpi.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageSerializer;
import edu.iu.dsc.tws.comms.mpi.io.SerializeState;
import edu.iu.dsc.tws.comms.mpi.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.mpi.io.types.KeySerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.MessageTypeUtils;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.MemoryManager;
import edu.iu.dsc.tws.data.memory.MemoryManagerContext;
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;

public class MPIDataFlowOperation implements MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowOperation.class.getName());

  // the configuration
  protected Config config;
  // the task plan
  protected TaskPlan instancePlan;

  protected int edge;
  // the router that gives us the possible routes
  protected TWSChannel channel;
  protected Map<Integer, MessageDeSerializer> messageDeSerializer;
  protected Map<Integer, MessageSerializer> messageSerializer;
  // we may have multiple routes throughus

  protected MessageType type;
  protected MessageType keyType = MessageType.BYTE;
  protected boolean isKeyed = false;
  protected Lock lock = new ReentrantLock();

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
//  protected Queue<Pair<Object, MPISendMessage>> pendingSendMessages;
  protected Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>>
      pendingSendMessagesPerSource;

  /**
   * Pending receives in case the receives are not ready
   */
  protected Map<Integer, Queue<Pair<Object, MPIMessage>>> pendingReceiveMessagesPerSource;

  protected Map<Integer, Queue<MPIMessage>> pendingReceiveDeSerializations;
  /**
   * Non grouped current messages
   */
  private Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected KryoSerializer kryoSerializer;

  protected boolean debug;

  protected Set<Integer> receivingExecutors;

  protected boolean isLastReceiver;

  protected MPIMessageReceiver receiver;

  /**
   * Memory manager that will be used to store buffers to memory store.
   */
  private MemoryManager memoryManager;

  /**
   * OperationMemoryManager for this instance.
   */
  private OperationMemoryManager operationMemoryManager;

  /**
   * Id of this operation. This will be used when storing the operation data in the memory store.
   */
  private int opertionID = 1234;

  private boolean isStoreBased = false;

  private ProgressionTracker sendProgressTracker;

  private ProgressionTracker receiveProgressTracker;

  private ProgressionTracker deserializeProgressTracker;

  private CompletionListener completionListener;

  private int sendBufferReleaseCount = 0;
  private int receiveBufferReleaseCount = 0;
  private int sendCount = 0;
  private int receiveCount = 0;
  private int sendsOfferred = 0;
  private int sendsPartialOfferred = 0;

  private int partialSendAttempts = 0;
  private int sendAttempts = 0;

  private long time1 = 0;
  private long time2 = 0;
  private int count = 0;

  private Map<Integer, Integer> sendSequence = new HashMap<>();
  private Map<Integer, Integer> sendPartialSequence = new HashMap<>();

  private ThreadLocal<ByteBuffer> threadLocalDataBuffer = new ThreadLocal<ByteBuffer>() {
    @Override
    protected ByteBuffer initialValue() {
      return ByteBuffer.allocateDirect(MemoryManagerContext.TL_DATA_BUFF_INIT_CAP);
    }
  };

  private ThreadLocal<ByteBuffer> threadLocalKeyBuffer = new ThreadLocal<ByteBuffer>() {
    @Override
    protected ByteBuffer initialValue() {
      return ByteBuffer.allocateDirect(MemoryManagerContext.TL_KEY_BUFF_INIT_CAP);
    }
  };

  public MPIDataFlowOperation(TWSChannel channel) {
    this.channel = channel;
  }

  /**
   * init method
   */
  public void init(Config cfg, MessageType messageType, TaskPlan plan,
                   int graphEdge, Set<Integer> recvExecutors,
                   boolean lastReceiver, MPIMessageReceiver msgReceiver,
                   Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>>
                       pendingSendPerSource,
                   Map<Integer, Queue<Pair<Object, MPIMessage>>> pRMPS,
                   Map<Integer, Queue<MPIMessage>> pendingReceiveDesrialize,
                   Map<Integer, MessageSerializer> serializer,
                   Map<Integer, MessageDeSerializer> deSerializer, boolean k) {
    this.config = cfg;
    this.instancePlan = plan;
    this.edge = graphEdge;
    this.type = messageType;
    this.debug = false;
    this.executor = instancePlan.getThisExecutor();
    this.receivingExecutors = recvExecutors;
    this.isLastReceiver = lastReceiver;
    this.receiver = msgReceiver;
    this.isKeyed = k;

    this.pendingReceiveMessagesPerSource = pRMPS;
    this.pendingSendMessagesPerSource = pendingSendPerSource;
    this.pendingReceiveDeSerializations = pendingReceiveDesrialize;

    this.messageSerializer = serializer;
    this.messageDeSerializer = deSerializer;

    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

//    LOG.info(String.format("%d Send buffer size: %d", executor, sendBufferSize));
    this.sendBuffers = new ArrayBlockingQueue<MPIBuffer>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }
    this.receiveBuffers = new HashMap<>();

    LOG.fine(String.format("%d setup communication", instancePlan.getThisExecutor()));
    // now setup the sends and receives
    setupCommunication();

    // initialize the serializers
    LOG.fine(String.format("%d setup initializers", instancePlan.getThisExecutor()));
    initSerializers();

    initProgressTrackers();
  }

  public void setCompletionListener(CompletionListener cmpListener) {
    this.completionListener = cmpListener;
  }

  protected void initSerializers() {
    // initialize the serializers
    for (MessageSerializer serializer : messageSerializer.values()) {
      serializer.init(config, sendBuffers, isKeyed);
    }
    for (MessageDeSerializer deSerializer : messageDeSerializer.values()) {
      deSerializer.init(config, isKeyed);
    }
  }

  private void initProgressTrackers() {
    Set<Integer> items = pendingSendMessagesPerSource.keySet();
//    LOG.info(String.format("%d pendingSendMessagesPerSource %s", executor, items));
    sendProgressTracker = new ProgressionTracker(items);

    Set<Integer> items1 = pendingReceiveMessagesPerSource.keySet();
//    LOG.info(String.format("%d pendingReceiveMessagesPerSource %s", executor, items1));
    receiveProgressTracker = new ProgressionTracker(items1);

    Set<Integer> items2 = pendingReceiveDeSerializations.keySet();
//    LOG.info(String.format("%d pendingReceiveDeSerializations %s", executor, items1));
    deserializeProgressTracker = new ProgressionTracker(items2);
  }

  /**
   * Setup the receives and send sendBuffers
   */
  protected void setupCommunication() {
    // we will receive from these
    int maxReceiveBuffers = MPIContext.receiveBufferCount(config);
    int receiveBufferSize = MPIContext.bufferSize(config);
//    LOG.info(String.format("%d Receive buffer size: %d", executor, receiveBufferSize));
    for (Integer recv : receivingExecutors) {
      Queue<MPIBuffer> recvList = new LinkedBlockingQueue<>();
      for (int i = 0; i < maxReceiveBuffers; i++) {
        recvList.add(new MPIBuffer(receiveBufferSize));
      }
      // register with the channel
      LOG.fine(instancePlan.getThisExecutor() + " Register to receive from: " + recv);
      channel.receiveMessage(recv, edge, this, recvList);
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

  public boolean sendMessagePartial(int source, Object object, int path,
                                    int flags, RoutingParameters routingParameters) {
    // for partial sends we use minus value to find the correct queue
    ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(source * -1 - 1);
    if (pendingSendMessages.remainingCapacity() > 0) {
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);
      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      // create a send message to keep track of the serialization
      // at the intial stage the sub-edge is 0
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean ret = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(object, sendMessage));
      if (!ret) {
        partialSendAttempts++;
      } else {
        ((TWSMPIChannel) channel).setDebug(false);
        partialSendAttempts = 0;
        sendsPartialOfferred++;
      }
      return ret;
    }
    return false;
  }

  public boolean sendMessage(int source, Object message, int path,
                             int flags, RoutingParameters routingParameters) {
    ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(source);
    if (pendingSendMessages.remainingCapacity() > 0) {
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);

      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean offer = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(message, sendMessage));
      if (!offer) {
        sendAttempts++;
      } else {
        ((TWSMPIChannel) channel).setDebug(false);
        sendAttempts = 0;
        sendsOfferred++;
      }
      return offer;
    }
    return false;
  }

  private void sendProgress(Queue<Pair<Object, MPISendMessage>> pendingSendMessages, int sendId) {
    boolean canProgress = true;
    while (pendingSendMessages.size() > 0 && canProgress) {
      // take out pending messages
      Pair<Object, MPISendMessage> pair = pendingSendMessages.peek();
      MPISendMessage mpiSendMessage = pair.getValue();
      Object messageObject = pair.getKey();
      if (mpiSendMessage.serializedState() == MPISendMessage.SendState.INIT) {
        // send it internally
        int startOfInternalRouts = mpiSendMessage.getAcceptedInternalSends();
        List<Integer> inRoutes = new ArrayList<>(mpiSendMessage.getInternalSends());
        for (int i = startOfInternalRouts; i < mpiSendMessage.getInternalSends().size(); i++) {
          //TODO: if this is the last task do we serialize internal messages and add it to
          //TODO: Memory Manager. it can be done here
          boolean receiveAccepted;
          if (isStoreBased && isLastReceiver) {
            serializeAndWriteToMemoryManager(mpiSendMessage, messageObject);
            receiveAccepted = receiver.receiveSendInternally(
                mpiSendMessage.getSource(), inRoutes.get(i), mpiSendMessage.getPath(),
                mpiSendMessage.getFlags(), operationMemoryManager);
            //send memory manager as reply
            //mpiSendMessage.setSerializationState();
          } else {
            lock.lock();
            try {
              receiveAccepted = receiver.receiveSendInternally(
                  mpiSendMessage.getSource(), inRoutes.get(i), mpiSendMessage.getPath(),
                  mpiSendMessage.getFlags(), messageObject);
            } finally {
              lock.unlock();
            }
          }

          if (!receiveAccepted) {
            canProgress = false;
            break;
          }
          mpiSendMessage.incrementAcceptedInternalSends();
        }
        if (canProgress) {
          mpiSendMessage.setSendState(MPISendMessage.SendState.SENT_INTERNALLY);
        }
      }

      if (canProgress) {
        // we don't have an external executor to send this message
        if (mpiSendMessage.getExternalSends().size() == 0) {
          pendingSendMessages.poll();
          continue;
        }
        //TODO: why build message after sent internally? is it for messages with multiple
        //TODO: destinations?
        // at this point lets build the message
        MPISendMessage message = (MPISendMessage)
            messageSerializer.get(sendId).build(pair.getKey(), mpiSendMessage);
        // okay we build the message, send it
        if (message.serializedState() == MPISendMessage.SendState.SERIALIZED) {
          List<Integer> exRoutes = new ArrayList<>(mpiSendMessage.getExternalSends());
          int startOfExternalRouts = mpiSendMessage.getAcceptedExternalSends();
          int noOfExternalSends = startOfExternalRouts;
          for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
            boolean sendAccepted = sendMessageToTarget(message.getMPIMessage(), exRoutes.get(i));
            // if no longer accepts stop
            if (!sendAccepted) {
              canProgress = false;

              break;
            } else {
              sendCount++;
              mpiSendMessage.incrementAcceptedExternalSends();
              noOfExternalSends++;
            }
          }

          if (noOfExternalSends == exRoutes.size()) {
            // we are done
            mpiSendMessage.setSendState(MPISendMessage.SendState.FINISHED);
            pendingSendMessages.poll();
          }
        } else {
          break;
        }
      }
    }
  }

  private void receiveDeserializeProgress(MPIMessage currentMessage, int receiveId) {
    if (currentMessage == null) {
      return;
    }

    int id = currentMessage.getOriginatingId();

    //If this is the last receiver we save to memory store
    if (isStoreBased && isLastReceiver) {
//      LOG.info("Store based");
      writeToMemoryManager(currentMessage, receiveId);
      currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
      if (!receiver.receiveMessage(currentMessage, operationMemoryManager)) {
        return;
      }
      currentMessage.incrementRefCount();
      currentMessage.release();
    } else {
      Object object = messageDeSerializer.get(receiveId).build(currentMessage,
          currentMessage.getHeader().getEdge());
      Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
          pendingReceiveMessagesPerSource.get(id);

      currentMessage.setReceivedState(MPIMessage.ReceivedState.INIT);
      if (!pendingReceiveMessages.offer(new ImmutablePair<>(object, currentMessage))) {
        throw new RuntimeException(executor + " We should have enough space: "
            + pendingReceiveMessages.size());
      }
    }
  }


  private void receiveProgress(Queue<Pair<Object, MPIMessage>> pendingReceiveMessages) {
    while (pendingReceiveMessages.size() > 0) {
      Pair<Object, MPIMessage> pair = pendingReceiveMessages.peek();
      MPIMessage.ReceivedState state = pair.getRight().getReceivedState();
      MPIMessage currentMessage = pair.getRight();
      Object object = pair.getLeft();

      if (state == MPIMessage.ReceivedState.INIT) {
        currentMessage.incrementRefCount();
      }

      lock.lock();
      try {
        if (state == MPIMessage.ReceivedState.DOWN || state == MPIMessage.ReceivedState.INIT) {
          currentMessage.setReceivedState(MPIMessage.ReceivedState.DOWN);
          if (!receiver.passMessageDownstream(object, currentMessage)) {
            break;
          }
          currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
          if (!receiver.receiveMessage(currentMessage, object)) {
            break;
          }
          currentMessage.release();
          pendingReceiveMessages.poll();
        } else if (state == MPIMessage.ReceivedState.RECEIVE) {
          currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
          if (!receiver.receiveMessage(currentMessage, object)) {
            break;
          }
          currentMessage.release();
          pendingReceiveMessages.poll();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Progress the serializations
   */
  public void progress() {
//    if (partialSendAttempts > 1000000 || sendAttempts > 1000000) {
//      String s = "";
//      for (Map.Entry<Integer, Queue<MPIBuffer>> e : receiveBuffers.entrySet()) {
//        s += e.getKey() + "-" + e.getValue().size() + " ";
//      }
//      LOG.info(String.format(
//          "%d send count %d receive %d send release %d receive release %d %s %d %d",
//          executor, sendCount, receiveCount, sendBufferReleaseCount,
//          receiveBufferReleaseCount, s, sendsOfferred, sendsPartialOfferred));
//      ((TWSMPIChannel) channel).setDebug(true);
//    }
    if (sendProgressTracker.canProgress()) {
      int sendId = sendProgressTracker.next();
      if (sendId != Integer.MIN_VALUE) {
        sendProgress(pendingSendMessagesPerSource.get(sendId), sendId);
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
        receiveProgress(pendingReceiveMessagesPerSource.get(receiveId));
        receiveProgressTracker.finish(receiveId);
      }
    }
  }

  /**
   * Serialize the message object and send it to store
   *
   * @param mpiSendMessage mpi send message that has information
   * @param messageObject object to be sent
   */
  private boolean serializeAndWriteToMemoryManager(MPISendMessage mpiSendMessage,
                                                   Object messageObject) {
    if (mpiSendMessage.getSerializationState() == null) {
      mpiSendMessage.setSerializationState(new SerializeState());
    }
    ByteBuffer tempData;
    ByteBuffer tempKey;

    if (isKeyed) {
      KeyedContent kc = (KeyedContent) messageObject;
      if (MessageTypeUtils.isMultiMessageType(kc.getKeyType())) {
        List<byte[]> keys = KeySerializer.getserializedMultiKey(kc.getSource(),
            mpiSendMessage.getSerializationState(), keyType, kryoSerializer);
        List<byte[]> data = DataSerializer.getserializedMultiData(kc.getObject(), type,
            mpiSendMessage.getSerializationState(), kryoSerializer);
        setupThreadLocalBuffers(keys.get(0).length, data.get(0).length, type);

        tempData = threadLocalDataBuffer.get();
        tempKey = threadLocalKeyBuffer.get();
        for (int i = 0; i < keys.size(); i++) {
          tempKey.put(keys.get(i));
          tempData.putInt(data.get(i).length);
          tempData.put(data.get(i));
          operationMemoryManager.put(tempKey, tempData);
          tempKey.clear();
          tempData.clear();
        }

      } else {
        byte[] keyBytes = KeySerializer.getserializedKey(kc.getSource(),
            mpiSendMessage.getSerializationState(), keyType, kryoSerializer);

        int dataLength = DataSerializer.serializeData(kc.getObject(), type,
            mpiSendMessage.getSerializationState(), kryoSerializer);
        setupThreadLocalBuffers(keyBytes.length, dataLength, type);

        tempData = threadLocalDataBuffer.get();
        tempKey = threadLocalKeyBuffer.get();

        tempKey.put(keyBytes);
        tempKey.flip();
        DataSerializer.getserializedData(kc.getObject(), type,
            mpiSendMessage.getSerializationState(), kryoSerializer, tempData);
        operationMemoryManager.put(tempKey, tempData);
      }
      return true;
    } else {
      //if this is not a keyed operation we will use the source task id as the key
      //Will be implmented after further looking into the use case
      int key = mpiSendMessage.getSource();
      int dataLength = DataSerializer.serializeData(messageObject, type,
          mpiSendMessage.getSerializationState(), kryoSerializer);

      setupThreadLocalBuffers(Integer.BYTES, dataLength, type);

      tempData = threadLocalDataBuffer.get();
      tempKey = threadLocalKeyBuffer.get();
      tempKey.putInt(key);
      DataSerializer.getserializedData(messageObject, type,
          mpiSendMessage.getSerializationState(), kryoSerializer, tempData);
      //TODO : need to generate operation key and use it
      return operationMemoryManager.put(tempKey, tempData);
    }
  }

  /**
   * Set's up all the thread local bytebuffers that are needed to copy the data into MM
   */
  private void setupThreadLocalBuffers(int keyLength, int dataLength, MessageType messageType) {
    //We are using a larger buffer anyway so no need to check for primitive just add 4 bytes

    int dataLengthTemp = dataLength + 4;
    /*if (!MessageTypeUtils.isPrimitiveType(messageType)) {
      dataLengthTemp = dataLength + 4;
    }*/
    //Using temp vars to reduce galls to threadlocal.get()
    ByteBuffer tempData = threadLocalDataBuffer.get();
    ByteBuffer tempKey = threadLocalKeyBuffer.get();
    if (keyLength < MemoryManagerContext.TL_KEY_BUFF_INIT_CAP
        && dataLengthTemp < MemoryManagerContext.TL_DATA_BUFF_INIT_CAP) {
      tempData.clear();
      tempKey.clear();
      return;
    }
    if (tempData.capacity() < dataLengthTemp) {
      tempData = ByteBuffer.allocateDirect(dataLengthTemp);
      threadLocalDataBuffer.set(tempData);
    }
    if (tempKey.capacity() < keyLength) {
      tempKey = ByteBuffer.allocateDirect(keyLength);
      threadLocalKeyBuffer.set(tempKey);
    }

    tempData.clear();
    tempKey.clear();
  }

  /**
   * extracts the data from the message and writes to the memory manager using the key
   *
   * @param currentMessage message to be parsed
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void writeToMemoryManager(MPIMessage currentMessage, int id) {
    Object data = messageDeSerializer.get(id).getDataBuffers(currentMessage, edge);
    //TODO: make sure to add the data length for non primitive types when adding to byte buffer
//    Object data = messageDeSerializer.get(id).build(currentMessage,
//        currentMessage.getHeader().getEdge());

    int sourceID = currentMessage.getHeader().getSourceId();
    int noOfMessages = 1;
    boolean isList = false;
    if (data instanceof List) {
      noOfMessages = ((List) data).size();
      isList = true;
    }

    ByteBuffer tempData;
    ByteBuffer tempKey;

    if (isList) {
      List objectList = (List) data;
      for (Object message : objectList) {
        if (isKeyed) {
          ImmutablePair<byte[], byte[]> tempPair = (ImmutablePair<byte[], byte[]>) message;
          setupThreadLocalBuffers(tempPair.getKey().length, tempPair.getValue().length,
              currentMessage.getType());

          tempData = threadLocalDataBuffer.get();
          tempKey = threadLocalKeyBuffer.get();

          tempKey.put(tempPair.getKey());
          tempData.putInt(tempPair.getValue().length);
          tempData.put(tempPair.getValue());
          operationMemoryManager.put(tempKey, tempData);
        } else {
          byte[] dataBytes = (byte[]) message;
          setupThreadLocalBuffers(4, dataBytes.length, currentMessage.getType());
          tempData = threadLocalDataBuffer.get();
          tempKey = threadLocalKeyBuffer.get();

          tempKey.putInt(sourceID);
          tempData.put(dataBytes);
          operationMemoryManager.put(tempKey, tempData);
        }
      }
    } else {
      if (isKeyed) {
        Pair<byte[], byte[]> tempPair = (Pair<byte[], byte[]>) data;
        setupThreadLocalBuffers(tempPair.getKey().length, tempPair.getValue().length,
            currentMessage.getType());

        tempData = threadLocalDataBuffer.get();
        tempKey = threadLocalKeyBuffer.get();

        tempKey.put(tempPair.getKey());
        tempData.putInt(tempPair.getValue().length);
        tempData.put(tempPair.getValue());
        operationMemoryManager.put(tempKey, tempData);
      } else {
        byte[] dataBytes = (byte[]) data;
        setupThreadLocalBuffers(4, dataBytes.length, currentMessage.getType());
        tempData = threadLocalDataBuffer.get();
        tempKey = threadLocalKeyBuffer.get();

        tempKey.putInt(sourceID);
        tempData.put(dataBytes);
        operationMemoryManager.put(tempKey, tempData);
      }
    }
  }

  private boolean sendMessageToTarget(MPIMessage mpiMessage, int i) {
    mpiMessage.incrementRefCount();
    int e = instancePlan.getExecutorForChannel(i);
    return channel.sendMessage(e, mpiMessage, this);
  }

  @Override
  public void release(MPIMessage message) {
    if (message.doneProcessing()) {
      int originatingId = message.getOriginatingId();
      releaseTheBuffers(originatingId, message);
    }
  }

  @Override
  public void onSendComplete(int id, int messageStream, MPIMessage message) {
    // ok we don't have anything else to do
    message.release();
  }

  protected void releaseTheBuffers(int id, MPIMessage message) {
    if (MPIMessageDirection.IN == message.getMessageDirection()) {
      Queue<MPIBuffer> list = receiveBuffers.get(id);
      for (MPIBuffer buffer : message.getBuffers()) {
        // we need to reset the buffer so it can be used again
        buffer.getByteBuffer().clear();
        if (!list.offer(buffer)) {
          throw new RuntimeException(String.format("%d Buffer release failed for target %d",
              executor, message.getHeader().getDestinationIdentifier()));
        }
        receiveBufferReleaseCount++;
      }
      if (completionListener != null) {
        completionListener.completed(message.getOriginatingId());
      }
    } else if (MPIMessageDirection.OUT == message.getMessageDirection()) {
      ArrayBlockingQueue<MPIBuffer> queue = (ArrayBlockingQueue<MPIBuffer>) sendBuffers;
      for (MPIBuffer buffer : message.getBuffers()) {
        // we need to reset the buffer so it can be used again
        buffer.getByteBuffer().clear();
        if (!queue.offer(buffer)) {
          String s = "";
          for (Map.Entry<Integer, Queue<MPIBuffer>> e : receiveBuffers.entrySet()) {
            s += e.getKey() + "-" + e.getValue().size() + " ";
          }
          LOG.info(String.format(
              "%d send count %d receive %d send release %d receive release %d %s %d %d",
              executor, sendCount, receiveCount, sendBufferReleaseCount,
              receiveBufferReleaseCount, s, sendsOfferred, sendsPartialOfferred));
          ((TWSMPIChannel) channel).setDebug(true);
          throw new RuntimeException(String.format("%d Buffer release failed for source %d %d %d",
              executor, message.getOriginatingId(), queue.size(), queue.remainingCapacity()));
        }
        sendBufferReleaseCount++;
      }
    }
  }


  @Override
  public void onReceiveComplete(int id, int e, MPIBuffer buffer) {
    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage currentMessage = currentMessages.get(id);
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    byteBuffer.position(buffer.getSize());
    byteBuffer.flip();
    receiveCount++;
    if (currentMessage == null) {
      currentMessage = new MPIMessage(id, type, MPIMessageDirection.IN, this);
      if (isKeyed) {
        currentMessage.setKeyType(keyType);
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
      Queue<MPIMessage> deserializeQueue = pendingReceiveDeSerializations.get(id);
      if (!deserializeQueue.offer(currentMessage)) {
        throw new RuntimeException(executor + " We should have enough space: "
            + deserializeQueue.size());
      }
    }
  }

  public TaskPlan getInstancePlan() {
    return instancePlan;
  }

  public Config getConfig() {
    return config;
  }

  public MessageType getType() {
    return type;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
    if (isStoreBased) {
      operationMemoryManager.setKeyType(MessageTypeUtils.toDataMessageType(keyType));
    }
  }

  public int getOpertionID() {
    return opertionID;
  }

  public void setOpertionID(int opertionID) {
    this.opertionID = opertionID;
  }

  public boolean isStoreBased() {
    return isStoreBased;
  }

  public void setStoreBased(boolean storeBased) {
    isStoreBased = storeBased;
    if (isStoreBased) {
      //TODO : need to load this from config file, both the type of memory manager and the datapath
      //TODO : need to make the memory manager available globally
      opertionID = (int) System.currentTimeMillis();
      this.kryoSerializer = new KryoSerializer();
//      Path dataPath = new Path("/scratch/pulasthi/terasort/lmdbdatabase_" + this.executor);
      Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbdatabase_" + this.executor);
      this.memoryManager = new LMDBMemoryManager(dataPath);
      if (!isKeyed) {
        this.operationMemoryManager = memoryManager.addOperation(opertionID,
            MessageTypeUtils.toDataMessageType(type));
      } else {
        this.operationMemoryManager = memoryManager.addOperation(opertionID,
            MessageTypeUtils.toDataMessageType(type),
            MessageTypeUtils.toDataMessageType(keyType));
      }
    }
  }
}
