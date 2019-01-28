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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.dfw.ChannelListener;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

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
    List<MPIRequest> pendingRequests;
    int rank;
    int edge;
    ChannelListener callback;
    Queue<DataBuffer> availableBuffers;

    MPIReceiveRequests(int rank, int e, ChannelListener callback, Queue<DataBuffer> buffers) {
      this.rank = rank;
      this.edge = e;
      this.callback = callback;
      this.availableBuffers = buffers;
      this.pendingRequests = new ArrayList<>();
    }
  }

  private class MPISendRequests {
    List<MPIRequest> pendingSends;
    int rank;
    int edge;
    ChannelMessage message;
    ChannelListener callback;

    MPISendRequests(int rank, int e, ChannelMessage message, ChannelListener callback) {
      this.rank = rank;
      this.edge = e;
      this.message = message;
      pendingSends = new ArrayList<>();
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
   * Wait for completion sends
   */
  private List<MPISendRequests> waitForCompletionSends;

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
   * Constructs the MPI channel
   * @param config configuration
   * @param comm communicator
   * @param worker worker id
   */
  public TWSMPIChannel(Config config, Intracomm comm, int worker) {
    this.comm = comm;
    int pendingSize = DataFlowContext.networkChannelPendingSize(config);
    this.pendingSends = new ArrayBlockingQueue<MPISendRequests>(pendingSize);
    this.registeredReceives = Collections.synchronizedList(new ArrayList<>(1024));
    this.waitForCompletionSends = Collections.synchronizedList(new ArrayList<>(1024));
    this.workerId = worker;
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
  public boolean receiveMessage(int wId, int e,
                                ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
    return registeredReceives.add(new MPIReceiveRequests(wId, e, callback, receiveBuffers));
  }

  @Override
  public void close() {
    // nothing to do here
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

  /**
   * Progress the communications that are pending
   */
  public void progress() {
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

    for (int i = 0; i < registeredReceives.size(); i++) {
      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
      if (debug) {
        LOG.info(String.format("%d available receive %d %d %s", workerId, receiveRequests.rank,
            receiveRequests.availableBuffers.size(), receiveRequests.availableBuffers.peek()));
      }
      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
        postReceive(receiveRequests);
      }
    }

    Iterator<MPISendRequests> sendRequestsIterator = waitForCompletionSends.iterator();
    boolean canProgress = true;
    while (sendRequestsIterator.hasNext() && canProgress) {
      MPISendRequests sendRequests = sendRequestsIterator.next();
      Iterator<MPIRequest> requestIterator = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
        MPIRequest r = requestIterator.next();
        try {
          Status status = r.request.testStatus();
          // this request has finished
          if (status != null) {
            completedSendCount++;
            requestIterator.remove();
          } else {
            canProgress = false;
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

    if (debug) {
      LOG.info(String.format(
          "%d sending - sent %d comp send %d receive %d pend recv %d pending sends %d waiting %d",
          workerId, sendCount, completedSendCount, receiveCount,
          pendingReceiveCount, pendingSends.size(), waitForCompletionSends.size()));
    }

    for (int i = 0; i < registeredReceives.size(); i++) {
      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
      try {
        Iterator<MPIRequest> requestIterator = receiveRequests.pendingRequests.iterator();
        while (requestIterator.hasNext()) {
          MPIRequest r = requestIterator.next();
          Status status = r.request.testStatus();
          if (status != null) {
            if (!status.isCancelled()) {
              ++receiveCount;
              // lets call the callback about the receive complete
              r.buffer.setSize(status.getCount(MPI.BYTE));

              if (receiveRequests.pendingRequests.size() == 0
                  && receiveRequests.availableBuffers.size() == 0) {
                //We do not have any buffers to receive messages so we need to free a buffer
                receiveRequests.callback.onReceiveComplete(
                    receiveRequests.rank, receiveRequests.edge, r.buffer, true);
              } else {
                receiveRequests.callback.onReceiveComplete(
                    receiveRequests.rank, receiveRequests.edge, r.buffer, false);
              }
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
          for (MPIRequest r : receiveRequests.pendingRequests) {
            try {
              r.request.cancel();
            } catch (MPIException e) {
              LOG.log(Level.WARNING, String.format("MPI Receive cancel error: rank %d edge %d",
                  closeRequest.getLeft(), closeRequest.getRight()));
            }
          }
          receiveRequests.pendingRequests.clear();
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

