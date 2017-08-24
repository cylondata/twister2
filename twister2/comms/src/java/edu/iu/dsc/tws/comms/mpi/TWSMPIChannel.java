package edu.iu.dsc.tws.comms.mpi;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

/**
 * We are going to use a byte based messaging protocol.
 */
public class TWSMPIChannel implements ReceiveCallback, SendCallback {
  private static final Logger LOG = Logger.getLogger(TWSMPIChannel.class.getName());

  /**
   * The MPI communicator
   */
  private final Intracomm comm;

  /**
   * The loop that progresses the communications
   */
  private final TWSMPILoop loop;

  /**
   * Holds the receive buffers. We can only submit receive requests until we have buffers
   * available in the pool.
   */
  private final ReceiveBufferPool receiveBufferPool;

  /**
   * If the receive buffer pool is occupied, we need to buffer the requests until we get buffers
   */
  private Map<Integer, PendingReceive> pendingReceives = new HashMap<>();

  public TWSMPIChannel(Intracomm comm, ReceiveBufferPool pool, TWSMPILoop loop) {
    this.comm = comm;
    this.receiveBufferPool = pool;
    this.loop = loop;
  }

  public void sendMessage(int rank, MPIMessage message, int stream) {
    for (int i = 0; i < message.getBuffers().size(); i++) {
      try {
        Request request = comm.iSend(message.getBuffers().get(i).getByteBuffer(), 0,
            MPI.BYTE, message.getBuffers().get(i).getSize(), stream);
        // register to the loop to make progress on the send
        loop.registerWrite(rank, request);
      } catch (MPIException e) {
        throw new RuntimeException("Failed to send message to rank: " + rank);
      }
    }
  }

  /**
   *
   * @param rank
   * @param callback
   * @param stream
   */
  public void receiveMessage(int rank, ReceiveCallback callback, int stream) {
    MPIBuffer byteBuffer = receiveBufferPool.getByteBuffer();
    if (byteBuffer == null) {
      pendingReceives.put(rank, new PendingReceive(rank, callback));
      return;
    } else {
      // post the receive
      Request request = postReceive(rank, stream, byteBuffer);
      loop.registerRead(rank, request);
      // update the pending
      addPending(rank, callback);
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

  private void addPending(int id, ReceiveCallback callback) {
    PendingReceive receive;
    if (pendingReceives.containsKey(id)) {
      receive = pendingReceives.get(id);
    } else {
      receive = new PendingReceive(id, callback);
      pendingReceives.put(id, receive);
    }
    receive.noOfBuffersSubmitted++;
  }

  /**
   * Handle message receives
   * @param id the id from which we receive the message
   * @param message the message
   */
  @Override
  public void onMessage(int id, MPIMessage message) {

  }

  /**
   * Handle the finish of a send request
   * @param id
   * @param request
   */
  @Override
  public void onFinish(int id, MPIRequest request) {

  }

  /**
   * Keep track of the receiving request
   */
  private class PendingReceive {
    private int id;
    private int noOfBuffersSubmitted;
    private ReceiveCallback callback;

    PendingReceive(int id, ReceiveCallback callback) {
      this.id = id;
      this.callback = callback;
    }
  }
}

