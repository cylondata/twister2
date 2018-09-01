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
package edu.iu.dsc.tws.examples.internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;


public class BasicNetworkTest implements IWorker, Runnable {
  private static final Logger LOG = Logger.getLogger(BasicNetworkTest.class.getName());

  private WorkerNetworkInfo workerNetworkInfo;
  private IWorkerController workerController;

  @Override
  public void execute(Config config,
                      int workerID,
                      AllocatedResources allocatedResources,
                      IWorkerController wController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {


    this.workerController = wController;
    workerNetworkInfo = wController.getWorkerNetworkInfo();

    LOG.info("Worker started: " + workerNetworkInfo);

    Thread echoServer = new Thread(this);
    echoServer.start();

    // wait for all workers in this job to join
    List<WorkerNetworkInfo> workerList = wController.waitForAllWorkersToJoin(50000);
    if (workerList != null) {
      LOG.info("All workers joined. " + WorkerNetworkInfo.workerListAsString(workerList));
    } else {
      LOG.severe("Can not get all workers to join. Exiting ........................");
      return;
    }

    // wait all echoServers to start
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    for (WorkerNetworkInfo worker : workerList) {
      if (worker.equals(workerNetworkInfo)) {
        continue;
      }

      sendReceiveHello(worker);
    }

    K8sWorkerUtils.waitIndefinitely();

  }

  /**
   * an echo server.
   */
  public void run() {

    // create socket
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(workerNetworkInfo.getWorkerPort());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not start ServerSocket.", e);
    }

    LOG.info("Echo Started server on port " + workerNetworkInfo.getWorkerPort());

    // repeatedly wait for connections, and process
    while (true) {

      try {
        // a "blocking" call which waits until a connection is requested
        Socket clientSocket = serverSocket.accept();
//        LOG.info("Accepted a connection from the client:" + clientSocket.getInetAddress());

        InputStream is = clientSocket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

        out.println("hello from the server: " + workerNetworkInfo);
        out.println("Will echo your messages:");

        String receivedMessage = reader.readLine();
        out.println(receivedMessage);

        out.flush();

        // close IO streams, then socket
//      LOG.info("received message:\n" + receivedMessage + "\nClosing the connection with client");
        out.close();
        reader.close();
        clientSocket.close();

      } catch (IOException ioe) {
        throw new IllegalArgumentException(ioe);
      }
    }
  }


  /**
   * send a hello message and receive the response
   */
  private void sendReceiveHello(WorkerNetworkInfo targetWorker) {

    try {
      Socket socketClient = new Socket(targetWorker.getWorkerIP(), targetWorker.getWorkerPort());
      LOG.info("Connection Established to: " + targetWorker);

      BufferedReader reader =
          new BufferedReader(new InputStreamReader(socketClient.getInputStream()));

      BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(socketClient.getOutputStream()));
      writer.write("hello from: " + workerNetworkInfo + "\n");
      writer.flush();

      String serverMessage = "";
      String message;
      while ((message = reader.readLine()) != null) {
        serverMessage += message + "\n";
      }

      LOG.info("\n" + serverMessage);

      reader.close();
      writer.close();
      socketClient.close();

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when trying to connect to: " + targetWorker, e);
    }
  }

}
