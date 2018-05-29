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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.system.JobExecutionState;

public class JobMaster {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  private static ExecutorService threadsPool;
  private static Progress looper;
  private RRServer rrServer;

  public void runServer(Config config, String host, int port, int workerID) {
    ServerConnectHandler serverConnectHandler = new ServerConnectHandler();

    rrServer = new RRServer(config, host, port, looper, workerID, serverConnectHandler);

    JobExecutionState.JobState.Builder jobStateBuilder = JobExecutionState.JobState.newBuilder();
    RequestMessageHandler requestMessageHandler = new RequestMessageHandler();
    rrServer.registerRequestHandler(jobStateBuilder, requestMessageHandler);

    rrServer.start();
  }

  public void close() {
    threadsPool.shutdownNow();
  }

  class RequestMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      System.out.println("request message received from workerID: " + workerId);
      System.out.println(message);

      JobExecutionState.JobState jobState = JobExecutionState.JobState.newBuilder()
          .setCluster("kubernetes-cluster")
          .setJobName("basic-kubernetes")
          .setJobId("job-server")
          .setSubmissionUser("user-y")
          .setSubmissionTime(System.currentTimeMillis())
          .build();

      rrServer.sendResponse(id, jobState);
      System.out.println("sent response: " + jobState);

    }
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      try {
        LOG.log(Level.INFO, "Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

  public static void main(String[] args) {
    Config config = null;
    String host = "localhost";
    int port = 11111;
    int workerID = 1;

    threadsPool = Executors.newSingleThreadExecutor();
    looper = new Progress();

    JobMaster jobMaster = new JobMaster();
    jobMaster.runServer(config, host, port, workerID);

    long runningTime = 100;
    LOG.info("JobMaster started on port " + port + " will run " + runningTime + " seconds");

    runningTime *= 1000;
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < runningTime) {
      looper.loop();
    }

    jobMaster.close();
  }

}
