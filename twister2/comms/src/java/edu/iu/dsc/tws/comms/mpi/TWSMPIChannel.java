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
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.api.comms.channel.ChannelListener;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.util.IterativeLinkedList;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

/**
 * We are going to use a byte based messaging protocol.
 * <p>
 * The transport threads doesn't handle the message serialization and it is left to the
 * application level.
 */
@SuppressWarnings("VisibilityModifier")
public class TWSMPIChannel implements TWSChannel {
  private static final Logger LOG = Logger.getLogger(TWSMPIChannel.class.getName());

  private class MPIRequest {
    Request request;
    DataBuffer buffer;

    MPIRequest(Request request, DataBuffer buffer) {
      this.request = request;
      this.buffer = buffer;
    }
  }

  private class MPIReceiveRequests {
    IterativeLinkedList<MPIRequest> pendingRequests;
    int rank;
    int edge;
    ChannelListener callback;
    Queue<DataBuffer> availableBuffers;

    MPIReceiveRequests(int rank, int e, ChannelListener callback, Queue<DataBuffer> buffers) {
      this.rank = rank;
      this.edge = e;
      this.callback = callback;
      this.availableBuffers = buffers;
      this.pendingRequests = new IterativeLinkedList<>();
    }
  }

  private class MPISendRequests {
    IterativeLinkedList<MPIRequest> pendingSends;
    int rank;
    int edge;
    ChannelMessage message;
    ChannelListener callback;

    MPISendRequests(int rank, int e, ChannelMessage message, ChannelListener callback) {
      this.rank = rank;
      this.edge = e;
      this.message = message;
      pendingSends = new IterativeLinkedList<>();
      this.callback = callback;
    }
  }

  /**
   * The MPI communicator
   */
  private final Intracomm comm;

  /**
   * Pending sends waiting to be posted
   */
  private ArrayBlockingQueue<MPISendRequests> pendingSends;

  /**
   * These are the places where we expect to receive messages
   */
  private List<MPIReceiveRequests> registeredReceives;

  /**
   * The grouped receives
   */
  private Int2ObjectArrayMap<List<MPIReceiveRequests>> groupedRegisteredReceives;

  /**
   * Wait for completion sends
   */
  private IterativeLinkedList<MPISendRequests> waitForCompletionSends;

  /**
   * Holds requests that are pending for close
   */
  private List<Pair<Integer, Integer>> pendingCloseRequests = new ArrayList<>();

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Some debug counters
   */
  private int sendCount = 0;
  private int completedSendCount = 0;
  private int receiveCount = 0;
  private int pendingReceiveCount = 0;
  private boolean debug = false;
  private int completedReceives = 0;

  /**
   * Create the mpi channel
   * @param config configuration
   * @param wController controller
   */
  public TWSMPIChannel(Config config,
                       IWorkerController wController) {
    Object commObject = wController.getRuntimeObject("comm");
    if (commObject == null) {
      this.comm = MPI.COMM_WORLD;
    } else {
      this.comm = (Intracomm) commObject;
    }
    int pendingSize = DataFlowContext.networkChannelPendingSize(config);
    this.pendingSends = new ArrayBlockingQueue<>(pendingSize);
    this.registeredReceives = new ArrayList<>(1024);
    this.groupedRegisteredReceives = new Int2ObjectArrayMap<>();
    this.waitForCompletionSends = new IterativeLinkedList<>();
    this.workerId = wController.getWorkerInfo().getWorkerID();
  }

  /**
   * Send messages to the particular id
   *
   * @param wId id to be used for sending messages
   * @param message the message
   * @return true if the message is accepted to be sent
   */
  public boolean sendMessage(int wId, ChannelMessage message, ChannelListener callback) {
    return pendingSends.offer(
        new MPISendRequests(wId, message.getHeader().getEdge(), message, callback));
  }

  /**
   * Register our interest to receive messages from particular rank using a stream
   *
   * @param wId worker id to listen to
   * @return true if the message is accepted
   */
  public boolean receiveMessage(int group, int wId, int e,
                                ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
    // add the request to both lists
    MPIReceiveRequests requests = new MPIReceiveRequests(wId, e, callback, receiveBuffers);
    registeredReceives.add(requests);
    List<MPIReceiveRequests> list;
    if (groupedRegisteredReceives.containsKey(group)) {
      list = groupedRegisteredReceives.get(group);
    } else {
      list = new ArrayList<>();
      groupedRegisteredReceives.put(group, list);
    }
    list.add(requests);
    return true;
  }

  @Override
  public void close() {
    // nothing to do here
    while (!this.pendingCloseRequests.isEmpty()
        || !this.pendingSends.isEmpty() || !this.waitForCompletionSends.isEmpty()) {
      this.progress();
    }
  }

  @Override
  public boolean isComplete() {
    // nothing to do here
    return this.pendingCloseRequests.isEmpty()
        && this.pendingSends.isEmpty() && this.waitForCompletionSends.isEmpty();
  }

  /**
   * Send a message to the given rank.
   *
   * @param requests the message
   */
  private void postMessage(MPISendRequests requests) {
    ChannelMessage message = requests.message;
    for (int i = 0; i < message.getNormalBuffers().size(); i++) {
      try {
        sendCount++;
        DataBuffer buffer = message.getNormalBuffers().get(i);
        Request request = comm.iSend(buffer.getByteBuffer(), buffer.getSize(),
            MPI.BYTE, requests.rank, message.getHeader().getEdge());
        // register to the loop to make communicationProgress on the send
        requests.pendingSends.add(new MPIRequest(request, buffer));
      } catch (MPIException e) {
        throw new RuntimeException("Failed to send message to rank: " + requests.rank);
      }
    }
  }

  private void postReceive(MPIReceiveRequests requests) {
    DataBuffer byteBuffer = requests.availableBuffers.poll();
    while (byteBuffer != null) {
      // post the receive
      pendingReceiveCount++;
      Request request = postReceive(requests.rank, requests.edge, byteBuffer);
      requests.pendingRequests.add(new MPIRequest(request, byteBuffer));
      byteBuffer = requests.availableBuffers.poll();
    }
  }

  /**
   * Post the receive request to MPI
   *
   * @param rank MPI rank
   * @param stream the stream as a tag
   * @param byteBuffer the buffer
   * @return the request
   */
  private Request postReceive(int rank, int stream, DataBuffer byteBuffer) {
    try {
      return comm.iRecv(byteBuffer.getByteBuffer(), byteBuffer.getCapacity(),
          MPI.BYTE, rank, stream);
    } catch (MPIException e) {
      throw new RuntimeException("Failed to post the receive", e);
    }
  }

  @Override
  public void progressSends() {
    // we should rate limit here
    while (pendingSends.size() > 0) {
      // post the message
      MPISendRequests sendRequests = pendingSends.poll();
      // post the send
      if (sendRequests != null) {
        postMessage(sendRequests);
        waitForCompletionSends.add(sendRequests);
      }
    }

    IterativeLinkedList.ILLIterator sendRequestsIterator
        = waitForCompletionSends.iterator();
    while (sendRequestsIterator.hasNext()) {
      MPISendRequests sendRequests = (MPISendRequests) sendRequestsIterator.next();
      IterativeLinkedList.ILLIterator requestIterator
          = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
        MPIRequest r = (MPIRequest) requestIterator.next();
        try {
          Status status = r.request.testStatus();
          // this request has finished
          if (status != null) {
//            completedSendCount++;
            requestIterator.remove();
          } else {
            break;
          }
        } catch (MPIException e) {
          throw new RuntimeException("Failed to complete the send to: " + sendRequests.rank, e);
        }
      }

      // if the message if fully sent, lets call the callback
      // ideally we should be able to call for each finish of the buffer
      if (sendRequests.pendingSends.size() == 0) {
        sendRequests.callback.onSendComplete(sendRequests.rank,
            sendRequests.edge, sendRequests.message);
        sendRequestsIterator.remove();
      }
    }
  }

  @Override
  public void progressReceives(int receiveGroupIndex) {
    progressInternalReceives(groupedRegisteredReceives.get(receiveGroupIndex));
    // if there are pending close requests, lets handle them
    handlePendingCloseRequests();
  }

  private void progressInternalReceives(List<MPIReceiveRequests> requests) {
    for (int i = 0; i < requests.size(); i++) {
      MPIReceiveRequests receiveRequests = requests.get(i);
      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
        postReceive(receiveRequests);
      }
      try {
        IterativeLinkedList.ILLIterator requestIterator
            = receiveRequests.pendingRequests.iterator();
        while (requestIterator.hasNext()) {
          MPIRequest r = (MPIRequest) requestIterator.next();
          Status status = r.request.testStatus();
          if (status != null) {
            if (!status.isCancelled()) {
//              ++receiveCount;
              // lets call the callback about the receive complete
              r.buffer.setSize(status.getCount(MPI.BYTE));

              //We do not have any buffers to receive messages so we need to free a buffer
              receiveRequests.callback.onReceiveComplete(
                  receiveRequests.rank, receiveRequests.edge, r.buffer);

              pendingReceiveCount--;
              requestIterator.remove();
            } else {
              throw new RuntimeException("MPI receive request cancelled");
            }
          } else {
            break;
          }
        }
        // this request has completed
      } catch (MPIException e) {
        LOG.log(Level.SEVERE, "Twister2Network failure", e);
        throw new RuntimeException("Twister2Network failure", e);
      }
    }
  }

  /**
   * Progress the communications that are pending
   */
  public void progress() {
    // first progress sends
    progressSends();
    // we progress all
    progressInternalReceives(registeredReceives);
    // if there are pending close requests, lets handle them
    handlePendingCloseRequests();
  }

  /**
   * Close all the pending requests
   */
  private void handlePendingCloseRequests() {
    while (pendingCloseRequests.size() > 0) {
      Pair<Integer, Integer> closeRequest = pendingCloseRequests.remove(0);

      // clear up the receive requests
      Iterator<MPIReceiveRequests> itr = registeredReceives.iterator();
      while (itr.hasNext()) {
        MPIReceiveRequests receiveRequests = itr.next();
        if (receiveRequests.edge == closeRequest.getRight()
            && receiveRequests.rank == closeRequest.getLeft()) {
          IterativeLinkedList.ILLIterator pendItr
              = receiveRequests.pendingRequests.iterator();
          while (pendItr.hasNext()) {
            try {
              MPIRequest request = (MPIRequest) pendItr.next();
              request.request.cancel();
              pendItr.remove();
            } catch (MPIException e) {
              LOG.log(Level.WARNING, String.format("MPI Receive cancel error: rank %d edge %d",
                  closeRequest.getLeft(), closeRequest.getRight()));
            }
          }
          itr.remove();
        }
      }
      // lets not handle any send requests as they will complete eventually
    }
  }

  @Override
  public ByteBuffer createBuffer(int capacity) {
    return MPI.newByteBuffer(capacity);
  }

  /**
   * Close a worker id with edge
   *
   * @param wId worker id
   * @param e edge
   */
  public void releaseBuffers(int wId, int e) {
    pendingCloseRequests.add(new ImmutablePair<>(wId, e));
  }
}

