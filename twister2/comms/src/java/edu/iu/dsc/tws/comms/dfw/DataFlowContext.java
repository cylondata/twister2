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
package edu.iu.dsc.tws.comms.dfw;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.CommunicationContext;

/**
 * Read the configuration options
 */
public class DataFlowContext extends CommunicationContext {
  private static final String BUFFER_SIZE = "network.mpi.buffer.size";

  private static final String SEND_BUFFERS_COUNT = "network.mpi.sendBuffer.count";
  private static final String BCAST_BUFFERS_COUNT = "network.mpi.bcast.sendBuffer.count";
  private static final String RECEIVE_BUFFERS_COUNT = "network.mpi.receiveBuffer.count";
  private static final String DISTINCT_ROUTS = "network.mpi.routing.distinct.routes";
  private static final String SEND_PENDING_MAX = "network.mpi.send.pending.max";
  private static final String NETWORK_CHANNEL_PENDING_SIZE = "network.channel.mpi.pending.size";
  private static final String NETWORK_STORAGE_PATH = "network.storage.path";
  private static final String NETWORK_PARTITION_MESSAGE_GROUP_LOW_WATERMARK =
      "network.partition.message.group.low_water_mark";
  private static final String NETWORK_PARTITION_MESSAGE_GROUP_HIGH_WATERMARK =
      "network.partition.message.group.high_water_mark";

  private static final String SHUFFLE_MAX_BYTES_IN_MEMORY = "shuffle.memory.bytes.max";
  private static final String SHUFFLE_MAX_RECORDS_IN_MEMORY = "shuffle.memory.records.max";

  public static final int DEFAULT_DESTINATION = 0;

  public static final List<Object> EMPTY_OBJECT = new ArrayList<>();

  public static int bufferSize(Config cfg) {
    return cfg.getIntegerValue(BUFFER_SIZE, 2048000);
  }

  public static int sendBuffersCount(Config cfg) {
    return cfg.getIntegerValue(SEND_BUFFERS_COUNT, 32);
  }

  public static int broadcastBufferCount(Config cfg) {
    int sendBufferCount = sendBuffersCount(cfg);
    return cfg.getIntegerValue(BCAST_BUFFERS_COUNT, sendBufferCount);
  }

  public static int receiveBufferCount(Config cfg) {
    return cfg.getIntegerValue(RECEIVE_BUFFERS_COUNT, 32);
  }

  public static int sendPendingMax(Config cfg) {
    return cfg.getIntegerValue(SEND_PENDING_MAX, 16);
  }

  public static int networkChannelPendingSize(Config cfg) {
    return cfg.getIntegerValue(NETWORK_CHANNEL_PENDING_SIZE, 1024);
  }

  public static String networkStoragePath(Config cfg) {
    return cfg.getStringValue(NETWORK_STORAGE_PATH);
  }

  public static int getNetworkPartitionMessageGroupLowWaterMark(Config cfg) {
    return cfg.getIntegerValue(NETWORK_PARTITION_MESSAGE_GROUP_LOW_WATERMARK, 8);
  }

  public static int getNetworkPartitionMessageGroupHighWaterMark(Config cfg) {
    return cfg.getIntegerValue(NETWORK_PARTITION_MESSAGE_GROUP_HIGH_WATERMARK, 16);
  }

  public static int getShuffleMaxRecordsInMemory(Config cfg) {
    return cfg.getIntegerValue(SHUFFLE_MAX_RECORDS_IN_MEMORY, 64);
  }

  public static int getShuffleMaxBytesInMemory(Config cfg) {
    return cfg.getIntegerValue(SHUFFLE_MAX_BYTES_IN_MEMORY, 6400);
  }
}

