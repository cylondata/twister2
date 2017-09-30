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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.core.CommunicationContext;

/**
 * Read the configuration options
 */
public class MPIContext extends CommunicationContext {
  private static final String BUFFER_SIZE = "network.mpi.buffer.size";

  private static final String SEND_BUFFERS_COUNT = "network.mpi.sendBuffer.count";
  private static final String BCAST_BUFFERS_COUNT = "network.mpi.bcast.sendBuffer.count";
  private static final String RECEIVE_BUFFERS_COUNT = "network.mpi.receiveBuffer.size";
  private static final String DISTINCT_ROUTS = "network.mpi.routing.distinct.routes";


  public static int bufferSize(Config cfg) {
    return cfg.getIntegerValue(BUFFER_SIZE, 1024);
  }

  public static int sendBuffersCount(Config cfg) {
    return cfg.getIntegerValue(SEND_BUFFERS_COUNT, 4);
  }

  public static int broadcastBufferCount(Config cfg) {
    int sendBufferCount = sendBuffersCount(cfg);
    return cfg.getIntegerValue(BCAST_BUFFERS_COUNT, sendBufferCount);
  }

  public static int receiveBufferCount(Config cfg) {
    return cfg.getIntegerValue(RECEIVE_BUFFERS_COUNT, 64);
  }

  public static int distinctRoutes(Config cfg, int defaultRoutes) {
    return cfg.getIntegerValue(DISTINCT_ROUTS, defaultRoutes);
  }
}

