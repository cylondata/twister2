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
package edu.iu.dsc.tws.master;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.system.JobExecutionState;

public class JobMasterClient {
  private static final Logger LOG = Logger.getLogger(JobMasterClient.class.getName());

  private static ExecutorService threadsPool;
  private static Progress looper;
  private RRClient rrClient;
  private ArrayList<RequestID> responseWaiters = new ArrayList<>();
  private static boolean responseReceived = false;

  public void runClient(Config config, String host, int port, int workerID) {
    ClientConnectHandler clientConnectHandler = new ClientConnectHandler();

    rrClient = new RRClient(host, port, config, looper, workerID, clientConnectHandler);

    JobExecutionState.JobState.Builder jobStateBuilder = JobExecutionState.JobState.newBuilder();
    ResponseMessageHandler responseMessageHandler = new ResponseMessageHandler();
    rrClient.registerResponseHandler(jobStateBuilder, responseMessageHandler);

    rrClient.start();
  }

  public void close() {
    threadsPool.shutdownNow();
  }

  class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      if (message instanceof JobExecutionState.JobState) {
        System.out.println("Received a response message with the requestID: " + id);
        System.out.println(message);

        if (responseWaiters.contains(id)) {
          responseWaiters.remove(id);
        } else {
          LOG.severe("Received a response that does not match any of the sent requestIDs. "
              + "received RequestID: " + id);
        }
      }
    }
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }

  public boolean sendMessage() {
    if (!rrClient.isConnected()) {
      return false;
    }

    JobExecutionState.JobState jobState = JobExecutionState.JobState.newBuilder()
        .setCluster("kubernetes-cluster")
        .setJobName("basic-kubernetes")
        .setJobId("job-client")
        .setSubmissionUser("user-y")
        .setSubmissionTime(System.currentTimeMillis())
        .build();

    RequestID requestID = rrClient.sendRequest(jobState);
    if (requestID == null) {
      return false;
    }

    responseWaiters.add(requestID);

    System.out.println("Sent the message with requestID: " + requestID + "\n" + jobState);
    return true;
  }

  public static void main(String[] args) {
    threadsPool = Executors.newSingleThreadExecutor();
    looper = new Progress();

    Config config = null;
    String serverHost = "localhost";
    int serverPort = 11111;
    int workerID = 0;

    JobMasterClient client = new JobMasterClient();
    client.runClient(config, serverHost, serverPort, workerID);

    long start = System.currentTimeMillis();
    long duration = 0;
    int messagesToSend = 10;
    long messageInterval = 1000;
    int messageCount = 0;

    while (messageCount < messagesToSend) {
      looper.loop();
      duration = System.currentTimeMillis() - start;
      if (duration > messageInterval) {
        start = System.currentTimeMillis();
        messageCount++;
        client.sendMessage();
      }
    }

    // wait for all responses to be received
    while (!client.responseWaiters.isEmpty()) {
      looper.loop();
    }

    client.close();

  }


}
