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
package edu.iu.dsc.tws.common.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;

public final class NetworkUtils {
  private static final Logger LOG = Logger.getLogger(NetworkUtils.class.getName());

  private NetworkUtils() {
  }

  /**
   * Get available port.
   *
   * @return available port. -1 if no available ports exist
   */
  public static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException ioe) {
      return -1;
    }
  }

  public static String printStackTrace() {
    String s = "";
    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    for (StackTraceElement e : elements) {
      s += e.toString() + "\n";
    }
    return s;
  }

  /**
   * Returns the list of free ports on localhost.
   * A ServerSocket is started on each port
   * These ports must be released after the JMWorkerAgent is started and
   * before IWorker is started in MPIWorkerManager/WorkerManager
   * using releaseWorkerPorts method
   *
   * @return a list of free ports
   */
  public static Map<String, Integer> findFreePorts(List<String> portNames) {
    List<ServerSocket> sockets = new ArrayList<>();
    Map<String, Integer> freePorts = new HashMap<>();
    try {
      for (String portName : portNames) {
        ServerSocket socket = new ServerSocket(0);
        socket.setReuseAddress(true);
        freePorts.put(portName, socket.getLocalPort());
        sockets.add(socket);
      }
      WorkerEnvironment.putSharedValue("socketsForFreePorts", sockets);
      return freePorts;
    } catch (IOException e) {
      throw new Twister2RuntimeException("Could not find free TCP/IP ports");
    }
  }

  /**
   * Release the ServerSockets on the local ports
   * that are started with findFreePorts method
   * This method must be called after the JMWorkerAgent is started and
   * before IWorker is started in MPIWorkerManager/WorkerManager
   */
  public static void releaseWorkerPorts() {

    List<ServerSocket> sockets =
        (List<ServerSocket>) WorkerEnvironment.removeSharedValue("socketsForFreePorts");
    boolean allSocketsClosed = true;
    int port = 0;
    for (ServerSocket socket : sockets) {
      try {
        port = socket.getLocalPort();
        socket.close();
        LOG.fine("Temporary socket closed at the port: " + port);
      } catch (IOException ioException) {
        allSocketsClosed = false;
        LOG.log(Level.SEVERE, "Exception when closing the temporary socket at the port: " + port,
            ioException);
      }
    }

    if (!allSocketsClosed) {
      throw new Twister2RuntimeException("Could not release one or more free TCP/IP ports");
    }
  }
}
