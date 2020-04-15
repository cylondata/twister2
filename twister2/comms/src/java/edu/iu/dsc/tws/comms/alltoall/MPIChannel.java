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
package edu.iu.dsc.tws.comms.alltoall;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

public class MPIChannel {
  private static final Logger LOG = Logger.getLogger(MPIChannel.class.getName());

  private static final int TWISTERX_CHANNEL_HEADER_SIZE = 8;
  private static final int TWISTERX_MSG_FIN = 1;

  private Intracomm comm;

  private Config cfg;

  private enum SendStatus {
    SEND_INIT,
    SEND_LENGTH_POSTED,
    SEND_POSTED,
    SEND_FINISH,
    SEND_DONE
  }

  private enum ReceiveStatus {
    RECEIVE_INIT,
    RECEIVE_LENGTH_POSTED,
    RECEIVE_POSTED,
    RECEIVED_FIN
  };

  private class PendingSend {
    IntBuffer headerBuf;
    Queue<TRequest> pendingData;
    SendStatus status = SendStatus.SEND_INIT;
    // the current send, if it is a actual send
    TRequest currentSend;
    Request request;
  }

  private class PendingReceive {
    // we allow upto 8 integer header
    IntBuffer headerBuf;
    int receiveId;
    ByteBuffer data;
    int length;
    ReceiveStatus status = ReceiveStatus.RECEIVE_INIT;
    // the current request
    Request request;
    // the user header
    int[] userHeader;
  };


  private int edge;
  // keep track of the length buffers for each receiver
  private Map<Integer, PendingSend> sends = new HashMap<>();
  // keep track of the posted receives
  private Map<Integer, PendingReceive> pendingReceives = new HashMap<>();
  // we got finish requests
  private Map<Integer, TRequest> finishRequests = new HashMap<>();
  // receive callback function
  private ChannelReceiveCallback receiveCallback;
  // send complete callback function
  private ChannelSendCallback sendCallback;
  // mpi rank
  private int rank;

  public MPIChannel(Config config,
                    IWorkerController wController) {
    this.cfg = config;
    Object commObject = wController.getRuntimeObject("comm");
    if (commObject == null) {
      this.comm = MPI.COMM_WORLD;
    } else {
      this.comm = (Intracomm) commObject;
    }
  }

  public void init(int ed, List<Integer> srcs, List<Integer> tgts,
                   ChannelReceiveCallback recvCallback, ChannelSendCallback sCallback) {
    this.edge = ed;
    this.receiveCallback = recvCallback;
    this.sendCallback = sCallback;
    // we need to post the length buffers
    for (int source : srcs) {
      PendingReceive pendingReceive = new PendingReceive();
      pendingReceive.receiveId = source;
      pendingReceives.put(source, pendingReceive);

      try {
        pendingReceive.request = comm.iRecv(pendingReceive.headerBuf, 8, MPI.INT, source, edge);
      } catch (MPIException e) {
        LOG.log(Level.SEVERE,"Failed to request", e);
        throw new RuntimeException(e);
      }
      // set the flag to true so we can identify later which buffers are posted
      pendingReceive.status = ReceiveStatus.RECEIVE_LENGTH_POSTED;
    }

    for (int target : tgts) {
      sends.put(target, new PendingSend());
    }
    // get the rank
    try {
      rank = comm.getRank();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE,"Failed to get mpi processes", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Send the request
   * @param request the request containing buffer, destination etc
   * @return if the request is accepted to be sent
   */
  public int send(TRequest request) {
    PendingSend send = sends.get(request.target);
    if (send.pendingData.offer(request)) {
      return 1;
    }
    return -1;
  }

  /**
   * Inform the finish to the target
   * @param request the request
   * @return -1 if not accepted, 1 if accepted
   */
  public int sendFin(TRequest request) {
    if (finishRequests.containsKey(request.target)) {
      LOG.log(Level.WARNING, "Sending finish to target twice " + request.target);
      return -1;
    }
    finishRequests.put(request.target, request);
    return 1;
  }

  /**
   * This method needs to be called to progress the send
   */
  public void progressSends() {
    try {
      // lets send values
      for (Map.Entry<Integer, PendingSend> x : sends.entrySet()) {
        boolean flag;
        // if we are in the length posted
        PendingSend pendSend = x.getValue();
        if (pendSend.status == SendStatus.SEND_LENGTH_POSTED) {
          flag = pendSend.request.test();
          if (flag) {
            pendSend.request = null;
            // now post the actual send
            TRequest r = pendSend.pendingData.peek();

            assert r != null;

            pendSend.request = comm.iSend(r.buffer, r.length, MPI.BYTE, r.target, edge);
            pendSend.status = SendStatus.SEND_POSTED;
            pendSend.pendingData.poll();
            // we set to the current send and pop it
            pendSend.currentSend = r;
          }
        } else if (pendSend.status == SendStatus.SEND_INIT) {
          pendSend.request = null;
          // now post the actual send
          if (!pendSend.pendingData.isEmpty()) {
            sendHeader(x.getValue());
          } else if (finishRequests.containsKey(x.getKey())) {
            // if there are finish requests lets send them
            sendFinishHeader(x.getValue());
          }
        } else if (pendSend.status == SendStatus.SEND_POSTED) {
          flag = pendSend.request.test();
          if (flag) {
            pendSend.request = null;
            // if there are more data to post, post the length buffer now
            if (!pendSend.pendingData.isEmpty()) {
              sendHeader(x.getValue());
              // we need to notify about the send completion
              sendCallback.sendComplete(pendSend.currentSend);
              pendSend.currentSend = null;
            } else {
              // we need to notify about the send completion
              sendCallback.sendComplete(pendSend.currentSend);
              pendSend.currentSend = null;
              // now check weather finish request is there
              if (finishRequests.containsKey(x.getKey())) {
                sendFinishHeader(x.getValue());
              } else {
                pendSend.status = SendStatus.SEND_INIT;
              }
            }
          }
        } else if (pendSend.status == SendStatus.SEND_FINISH) {
          flag = pendSend.request.test();
          if (flag) {
            // LOG(INFO) << rank << " FINISHED send " << x.first;
            // we are going to send complete
            TRequest finReq = finishRequests.get(x.getKey());
            sendCallback.sendFinishComplete(finReq);
            pendSend.status = SendStatus.SEND_DONE;
          }
        } else {
          // throw an exception and log
//        std::cout << "ELSE " << std::endl;
        }
      }
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Exception in MPI", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method needs to be called to progress the receives
   */
  public void progressReceives() {
    try {
      for (Map.Entry<Integer, PendingReceive> x : pendingReceives.entrySet()) {
        boolean flag;
        PendingReceive pendingReceive = x.getValue();
        if (pendingReceive.status == ReceiveStatus.RECEIVE_LENGTH_POSTED) {
          Status status = pendingReceive.request.testStatus();
          if (status != null) {
            pendingReceive.request = null;
            int count = status.getCount(MPI.INT);
            // read the length from the header
            int length = pendingReceive.headerBuf.get(0);
            int finFlag = pendingReceive.headerBuf.get(1);
            // LOG(INFO) << rank << " ** received " << length << " flag " << finFlag;
            // check weather we are at the end
            if (finFlag != TWISTERX_MSG_FIN) {
              if (count > 8) {
                LOG.log(Level.SEVERE,
                    "Un-expected number of bytes expected: 8 or less received: " + count);
              }
              // malloc a buffer
              pendingReceive.data = MPI.newByteBuffer(count);
              pendingReceive.length = length;
              pendingReceive.request = comm.iRecv(pendingReceive.data,
                  length, MPI.BYTE, pendingReceive.receiveId, edge);
              // LOG(INFO) << rank << " ** POST RECEIVE " << length << " addr: " << x.second->data;
              pendingReceive.status = ReceiveStatus.RECEIVE_POSTED;
              // copy the count - 2 to the buffer
              if (count > 2) {
                for (int i = 2; i < count; i++) {
                  pendingReceive.userHeader[i - 2] = pendingReceive.headerBuf.get(i);
                }
              }
              // notify the receiver
              receiveCallback.receivedHeader(x.getKey(), finFlag,
                  pendingReceive.userHeader, count - 2);
            } else {
              if (count != 2) {
                LOG.log(Level.SEVERE,
                    "Un-expected number of bytes expected: 2 received: " + count);
              }
              // we are not expecting to receive any more
              pendingReceive.status = ReceiveStatus.RECEIVED_FIN;
              // notify the receiver
              receiveCallback.receivedHeader(x.getKey(), finFlag, null, 0);
            }
          }
        } else if (pendingReceive.status == ReceiveStatus.RECEIVE_POSTED) {
          flag = pendingReceive.request.test();
          if (flag) {
            pendingReceive.request = null;
            // clear the array
            pendingReceive.headerBuf.clear();

            pendingReceive.request = comm.iRecv(pendingReceive.headerBuf,
                TWISTERX_CHANNEL_HEADER_SIZE, MPI.INT, pendingReceive.receiveId, edge);
            // LOG(INFO) << rank << " ** POST HEADER " << 8 << " addr: " << x.second->headerBuf;
            pendingReceive.status = ReceiveStatus.RECEIVE_LENGTH_POSTED;
            // call the back end
            receiveCallback.receivedData(x.getKey(), pendingReceive.data, pendingReceive.length);
          }
        } else {
          // we are at the end
        }
      }
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Error in MPI", e);
      throw new Twister2RuntimeException(e);
    }
  }

  private void sendHeader(PendingSend send) throws MPIException {
    TRequest r = send.pendingData.peek();
    assert send.pendingData.size() > 0;
    // put the length to the buffer
    send.headerBuf.put(0, r.length);
    send.headerBuf.put(1, 0);

    // copy the memory of the header
    if (r.headerLength > 0) {
      for (int i = 0; i < r.headerLength; i++) {
        send.headerBuf.put(i + 2, r.header[i]);
      }
    }
    // LOG(INFO) << rank << " Sent length to " << r->target << " addr: " << x.second->headerBuf << " len: " << r->headerLength + 2;
    // we have to add 2 to the header length
    send.request = comm.iSend(send.headerBuf, 2 + r.headerLength, MPI.INT, r.target, edge);
    send.status = SendStatus.SEND_LENGTH_POSTED;
  }

  private void sendFinishHeader(PendingSend send) throws MPIException {
    // for the last header we always send only the first 2 integers
    send.headerBuf.put(0, 0);
    send.headerBuf.put(1, TWISTERX_MSG_FIN);
    // LOG(INFO) << rank << " Sent finish to " << x.first;
    send.request = comm.iSend(send.headerBuf, 2, MPI.INT, send.currentSend.target, edge);
  }

  /**
   * Close the channel and clear any allocated memory by the channel
   */
  public void close() {

  }
}
