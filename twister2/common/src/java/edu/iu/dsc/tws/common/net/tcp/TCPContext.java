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
package edu.iu.dsc.tws.common.net.tcp;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.net.NetworkInfo;

public class TCPContext extends Context {
  public static final String TWISTER2_WRITE_SIZE = "twister2.tcp.write.size";

  public static final String TWISTER2_WRITE_TIME = "twister2.tcp.write.time";

  public static final String TWISTER2_READ_SIZE = "twister2.tcp.read.size";

  public static final String TWISTER2_READ_TIME = "twister2.tcp.read.time";

  public static final String TWISTER2_MAX_PACKET_SIZE = "twister2.tcp.packet.size";

  public static final String TWISTER2_SEND_BUFF_SIZE = "twister2.tcp.send.buffer.size";
  public static final String TWISTER2_RECV_BUFF_SIZE = "twister2.tcp.recv.buffer.size";

  public static final String NETWORK_HOSTNAME = "twister2.tcp.hostname";
  public static final String NETWORK_PORT = "twister2.tcp.port";

  public static int getNetworkWriteBatchSize(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_WRITE_SIZE, def);
  }

  public static int getNetworkWriteBatchTime(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_WRITE_TIME, def);
  }

  public static int getNetworkReadBatchSize(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_READ_SIZE, def);
  }

  public static int getNetworkReadBatchTime(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_READ_TIME, def);
  }

  public static int getSocketSendBufferSize(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_SEND_BUFF_SIZE, def);
  }

  public static int getSocketReceivedBufferSize(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_RECV_BUFF_SIZE, def);
  }

  public static int getMaximumPacketSize(Config cfg, int def) {
    return cfg.getIntegerValue(TWISTER2_MAX_PACKET_SIZE, def);
  }

  public static String getHostName(NetworkInfo networkInfo) {
    return (String) networkInfo.getProperties().get(NETWORK_HOSTNAME);
  }

  public static int getPort(NetworkInfo networkInfo) {
    return (int) networkInfo.getProperties().get(NETWORK_PORT);
  }
}
