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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

/**
 * We are going to use a byte based messaging protocol.
 *
 * The transport threads doesn't handle the message serialization and it is left to the
 * application level.
 */
public class TWSMPIChannel {
  private static final Logger LOG = Logger.getLogger(TWSMPIChannel.class.getName());

  // a lock object to be used
  private Lock lock = new ReentrantLock();

  private int executor;

  private int sendCount = 0;

  private int pendingSendCount = 0;

  @SuppressWarnings("VisibilityModifier")
  private class MPIRequest {
    Request request;
    MPIBuffer buffer;

    MPIRequest(Request request, MPIBuffer buffer) {
      this.request = request;
      this.buffer = buffer;
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class MPIReceiveRequests {
    List<MPIRequest> pendingRequests;
    int rank;
    int edge;
    MPIMessageListener callback;
    Queue<MPIBuffer> availableBuffers;

    MPIReceiveRequests(int rank, int e,
                              MPIMessageListener callback, Queue<MPIBuffer> buffers) {
      this.rank = rank;
      this.edge = e;
      this.callback = callback;
      this.availableBuffers = buffers;
      this.pendingRequests = new ArrayList<>();
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class MPISendRequests {
    List<MPIRequest> pendingSends;
    int rank;
    int edge;
    MPIMessage message;
    MPIMessageListener callback;

    MPISendRequests(int rank, int e,
                           MPIMessage message, MPIMessageListener callback) {
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


  public TWSMPIChannel(Config config, Intracomm comm, int exec) {
    this.comm = comm;
    this.pendingSends = new ArrayBlockingQueue<MPISendRequests>(1024);
    this.registeredReceives = Collections.synchronizedList(new ArrayList<>(1024));
    this.waitForCompletionSends = Collections.synchronizedList(new ArrayList<>(1024));
    this.executor = exec;
  }

  /**
   * Send messages to the particular id
   *
   * @param id id to be used for sending messages
   * @param message the message
   * @return true if the message is accepted to be sent
   */
  public boolean sendMessage(int id, MPIMessage message, MPIMessageListener callback) {
//    LOG.log(Level.INFO, "lock 1 " + executor);
//    lock.lock();
//    try {
    boolean offer = pendingSends.offer(
        new MPISendRequests(id, message.getHeader().getEdge(), message, callback));
    if (offer) {
      pendingSendCount++;
    }
//    LOG.info(String.format("%d Pending sends count: %d wait: %d",
//        executor, pendingSends.size(), waitForCompletionSends.size()));
    return offer;
//    } finally {
//      lock.unlock();
//    }
  }

  /**
   * Register our interest to receive messages from particular rank using a stream
   * @param rank
   * @param stream
   * @param callback
   * @return
   */
  public boolean receiveMessage(int rank, int stream,
                                MPIMessageListener callback, Queue<MPIBuffer> receiveBuffers) {
//    LOG.log(Level.INFO, "lock 2 " + executor);
//    lock.lock();
//    try {
    return registeredReceives.add(new MPIReceiveRequests(rank, stream, callback,
        receiveBuffers));
//    } finally {
////      lock.unlock();
//    }
  }

  /**
   * Send a message to the given rank.
   *
   * @param requests the message
   */
  private void postMessage(MPISendRequests requests) {
    MPIMessage message = requests.message;
    for (int i = 0; i < message.getBuffers().size(); i++) {
      try {
        sendCount++;
        MPIBuffer buffer = message.getBuffers().get(i);
//        LOG.info(String.format("%d Sending message to: %d size: %d sendCount: %d", executor,
//            requests.rank, buffer.getSize(), sendCount));
        Request request = comm.iSend(buffer.getByteBuffer(), buffer.getSize(),
            MPI.BYTE, requests.rank, message.getHeader().getEdge());
        // register to the loop to make progress on the send
        requests.pendingSends.add(new MPIRequest(request, buffer));
      } catch (MPIException e) {
        throw new RuntimeException("Failed to send message to rank: " + requests.rank);
      }
    }
  }

  private void postReceive(MPIReceiveRequests requests) {
    MPIBuffer byteBuffer = requests.availableBuffers.poll();
    if (byteBuffer != null) {
      // post the receive
      Request request = postReceive(requests.rank, requests.edge, byteBuffer);
      requests.pendingRequests.add(new MPIRequest(request, byteBuffer));
    }
  }

  /**
   * Post the receive request to MPI
   * @param rank MPI rank
   * @param stream the stream as a tag
   * @param byteBuffer the buffer
   * @return the request
   */
  private Request postReceive(int rank, int stream, MPIBuffer byteBuffer) {
    try {
      return comm.iRecv(byteBuffer.getByteBuffer(), byteBuffer.getCapacity(),
          MPI.BYTE, rank, stream);
    } catch (MPIException e) {
      throw new RuntimeException("Failed to post the receive", e);
    }
  }

  private int completedReceives = 0;
  /**
   * Progress the communications that are pending
   */
  public void progress() {
//    LOG.log(Level.INFO, "lock 3 " + executor);
//    lock.lock();
//    try {
//    LOG.log(Level.INFO, "Progress: pendingSends: " + pendingSends.size());
      // we should rate limit here
    while (pendingSends.size() > 0) {
//      LOG.info(String.format("%d Pending sends %d", executor, pendingSends.size()));
      // post the message
      MPISendRequests sendRequests = pendingSends.poll();
      // post the send
      postMessage(sendRequests);
      waitForCompletionSends.add(sendRequests);
    }

//    LOG.log(Level.INFO, "Pending sends done");
    for (int i = 0; i < registeredReceives.size(); i++) {
      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
//        LOG.info(String.format("%d Posting receive request: %d", executor, receiveRequests.rank));
        postReceive(receiveRequests);
      } /*else {
//          LOG.info("No receive buffers available for posting: " + receiveRequests.rank);
      }*/
    }

    Iterator<MPISendRequests> sendRequestsIterator = waitForCompletionSends.iterator();
    while (sendRequestsIterator.hasNext()) {
      MPISendRequests sendRequests = sendRequestsIterator.next();
      Iterator<MPIRequest> requestIterator = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
//        LOG.info("Loooping");
//        LOG.log(Level.INFO, "Waiting for completion sends");
        MPIRequest r = requestIterator.next();
        try {
//          LOG.info("Testing send status");
          Status status = r.request.testStatus();
          // this request has finished
          if (status != null) {
//            LOG.log(Level.INFO, executor + " Send finished");
            requestIterator.remove();
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


    for (int i = 0; i < registeredReceives.size(); i++) {

      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
//      LOG.info(String.format("%d Going through Pending receives: %d pending %d",
//          executor, registeredReceives.size(), receiveRequests.pendingRequests.size()));
      try {
        Iterator<MPIRequest> requestIterator = receiveRequests.pendingRequests.iterator();
        while (requestIterator.hasNext()) {
          MPIRequest r = requestIterator.next();
//          LOG.info(String.format("%d Testing receive status %d pending %d", executor,
//              receiveRequests.rank, receiveRequests.pendingRequests.size()));
          Status status = r.request.testStatus();
          if (status != null) {
            if (!status.isCancelled()) {
//              LOG.info(executor + " Receive completed: " + completedReceives++);
//               lets call the callback about the receive complete
              r.buffer.setSize(status.getCount(MPI.BYTE));
              receiveRequests.callback.onReceiveComplete(
                  receiveRequests.rank, receiveRequests.edge, r.buffer);
//              LOG.info(String.format("%d finished calling the on complete method", executor));
              requestIterator.remove();
            } else {
              throw new RuntimeException("MPI receive request cancelled");
            }
          }
        }
        // this request has completed
      } catch (MPIException e) {
        LOG.severe("Twister2Network failure");
        throw new RuntimeException("Twister2Network failure", e);
      }
    }
//    } finally {
//      lock.unlock();
//    }
  }
}

