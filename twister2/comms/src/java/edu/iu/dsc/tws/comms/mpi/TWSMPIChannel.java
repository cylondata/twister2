package edu.iu.dsc.tws.comms.mpi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

/**
 * We are going to use a byte based messaging protocol.
 */
public class TWSMPIChannel {
  private static final Logger LOG = Logger.getLogger(TWSMPIChannel.class.getName());

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
    int stream;
    MPIMessageListener callback;
    Queue<MPIBuffer> availableBuffers;

    MPIReceiveRequests(int rank, int stream,
                              MPIMessageListener callback, Queue<MPIBuffer> buffers) {
      this.rank = rank;
      this.stream = stream;
      this.callback = callback;
      this.availableBuffers = buffers;
      this.pendingRequests = new ArrayList<>();
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class MPISendRequests {
    List<MPIRequest> pendingSends;
    int rank;
    int stream;
    MPIMessage message;
    MPIMessageListener callback;

    MPISendRequests(int rank, int stream,
                           MPIMessage message, MPIMessageListener callback) {
      this.rank = rank;
      this.stream = stream;
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

  private List<MPIReceiveRequests> registeredReceives;

  private List<MPISendRequests> waitForCompletionSends;


  public TWSMPIChannel(Config config, Intracomm comm) {
    this.comm = comm;
    this.pendingSends = new ArrayBlockingQueue<MPISendRequests>(1024);
    this.registeredReceives = new ArrayList<>(1024);
    this.waitForCompletionSends = new ArrayList<>(1024);
  }

  /**
   * Send messages to the particular id
   *
   * @param id id to be used for sending messages
   * @param message the message
   * @return true if the message is accepted to be sent
   */
  public boolean sendMessage(int id, MPIMessage message, MPIMessageListener callback) {
    return pendingSends.offer(new MPISendRequests(id, message.getStream(), message, callback));
  }

  /**
   * Register our interest to receive messages from particular rank using a stream
   * @param rank
   * @param stream
   * @param callback
   * @return
   */
  public boolean receiveMessage(int rank, int stream,
                                MPIMessageListener callback, List<MPIBuffer> receiveBuffers) {
    return registeredReceives.add(new MPIReceiveRequests(rank, stream, callback,
        new LinkedList<MPIBuffer>(receiveBuffers)));
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
        MPIBuffer buffer = message.getBuffers().get(i);
        Request request = comm.iSend(buffer.getByteBuffer(), 0,
            MPI.BYTE, buffer.getSize(), message.getStream());
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
      Request request = postReceive(requests.rank, requests.stream, byteBuffer);
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
      return comm.iRecv(byteBuffer.getByteBuffer(), 0, MPI.BYTE, rank, stream);
    } catch (MPIException e) {
      throw new RuntimeException("Failed to post the receive", e);
    }
  }

  /**
   * Keep track of the receiving request
   */
  private class PendingReceive {
    private int id;
    private int noOfBuffersSubmitted;
    private MPIMessageListener callback;

    PendingReceive(int id, MPIMessageListener callback) {
      this.id = id;
      this.callback = callback;
    }
  }

  /**
   * Progress the communications
   */
  public void progress() {
    // we should rate limit here
    while (pendingSends.size() > 0) {
      // post the message
      MPISendRequests sendRequests = pendingSends.poll();
      // post the send
      postMessage(sendRequests);
      waitForCompletionSends.add(sendRequests);
    }

    for (int i = 0; i < registeredReceives.size(); i++) {
      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
        postReceive(receiveRequests);
      }
    }

    for (int i = 0; i < waitForCompletionSends.size(); i++) {
      MPISendRequests sendRequests = waitForCompletionSends.get(i);
      Iterator<MPIRequest> requestIterator = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
        MPIRequest r = requestIterator.next();
        try {
          Status status = r.request.testStatus();
          // this request has finished
          if (status != null) {
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
            sendRequests.stream, sendRequests.message);
      }
    }

    for (int i = 0; i < registeredReceives.size(); i++) {
      MPIReceiveRequests receiveRequests = registeredReceives.get(i);
      try {
        Iterator<MPIRequest> requestIterator = receiveRequests.pendingRequests.iterator();
        while (requestIterator.hasNext()) {
          MPIRequest r = requestIterator.next();
          Status status = r.request.testStatus();
          if (status != null) {
            // lets call the callback about the receive complete
            receiveRequests.callback.onReceiveComplete(
                receiveRequests.rank, receiveRequests.stream, r.buffer);
            requestIterator.remove();
          }
        }
        // this request has completed
      } catch (MPIException e) {
        LOG.severe("Network failure");
        throw new RuntimeException("Network failure");
      }
    }
  }
}

