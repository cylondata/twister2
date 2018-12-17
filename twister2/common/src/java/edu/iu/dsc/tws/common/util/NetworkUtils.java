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

public final class NetworkUtils {
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
   * Returns a map of free ports on localhost.
   *
   * @return a map from port name to port number
   * @throws IllegalStateException if unable to find specified free ports
   */
  public static Map<String, Integer> findFreePorts(List<String> portNames) {
    try {
      List<ServerSocket> serverSockets = new ArrayList<>();
      Map<String, Integer> freePorts = new HashMap<>();

      for (String portName : portNames) {
        ServerSocket socket = new ServerSocket(0);
        socket.setReuseAddress(true);
        freePorts.put(portName, socket.getLocalPort());
        serverSockets.add(socket);
      }

      for (ServerSocket socket : serverSockets) {
        socket.close();
      }
      return freePorts;
    } catch (IOException e) {
    }
    throw new IllegalStateException("Could not find a free TCP/IP port");
  }
}
