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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;

public class BasicMesosWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(BasicMesosContainer.class.getName());

  @Override
  public void execute(Config config, int workerID, AllocatedResources allocatedResources,
                      IWorkerController workerController, IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    LOG.info("Mesos Worker start time(ms): " + System.currentTimeMillis());

    // wait some random amount of time before finishing
    long duration = (long) (Math.random() * 1000);
    int port = workerController.getWorkerNetworkInfo().getWorkerPort();
    try {
      System.out.println("I am the worker: " + workerID);
      System.out.println("I am sleeping " + duration + "ms. Then will close.");
      Thread.sleep(duration);
      //echoServer(port);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  /***
   * echo server
   * @param port
   */
  public static void echoServer(int port) {

    // create socket
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
      System.out.println("listening on " + InetAddress.getLocalHost().getHostAddress() + ":"
          + serverSocket.getLocalPort());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // repeatedly wait for connections, and process
    while (true) {

      try {
        // a "blocking" call which waits until a connection is requested
        Socket clientSocket = serverSocket.accept();
        LOG.info("Accepted a connection from the client:" + clientSocket.getInetAddress());
        System.out.println("Accepted a connection from the client:"
            + clientSocket.getInetAddress() + "... now closing.");
        InputStream is = clientSocket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

        out.println("hello from the server: " + InetAddress.getLocalHost().getHostAddress() + ":"
            + serverSocket.getLocalPort());
        //out.println("Will echo your messages:");

        //String s;
        //while ((s = reader.readLine()) != null) {
        //out.println(s);
        //}

        // close IO streams, then socket
        LOG.info("Closing the connection with client");
        out.close();
        reader.close();
        clientSocket.close();
        break;

      } catch (IOException ioe) {
        throw new IllegalArgumentException(ioe);
      }
    }
  }
}
