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
package edu.iu.dsc.tws.api.comms;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.TokenSub;

public class CommunicationContext extends Context {
  public static final String REDUCE = "reduce";
  public static final String GATHER = "gather";
  public static final String PARTITION = "partition";
  public static final String ALLGATHER = "allgather";
  public static final String ALLREDUCE = "allreduce";
  public static final String BROADCAST = "broadcast";
  public static final String KEYED_PARTITION = "keyed_partition";
  public static final String KEYED_REDUCE = "keyed_reduce";
  public static final String KEYED_GATHER = "keyed_gather";
  public static final String DIRECT = "direct";
  public static final String JOIN = "join";
  public static final String TABLE_PARTITION = "table_partition";

  public static final String INTER_NODE_DEGREE = "twister2.network.routing.inter.node.degree";
  public static final String INTRA_NODE_DEGREE = "twister2.network.routing.intra.node.degree";
  public static final ByteOrder DEFAULT_BYTEORDER = ByteOrder.BIG_ENDIAN;
  public static final String PERSISTENT_DIRECTORIES = "twister2.network.ops.persistent.dirs";
  public static final String PERSISTENT_DIRECTORY_DEFAULT_VALUE = "${TWISTER2_HOME}/persistent/";

  public static final String ALLTOALL_ALGO_KEY =
      "twister2.network.alltoall.algorithm";
  public static final String ALLTOALL_ALGO_SIMPLE = "simple";
  public static final String ALLTOALL_ALGO_RING = "ring";

  public static final String BUFFER_SIZE = "twister2.network.buffer.size";

  public static final String SEND_BUFFERS_COUNT = "twister2.network.sendBuffer.count";
  public static final String RECEIVE_BUFFERS_COUNT = "twister2.network.receiveBuffer.count";
  public static final String SEND_PENDING_MAX = "twister2.network.send.pending.max";
  public static final String CHANNEL_PENDING_SIZE
      = "twister2.network.channel.pending.size";
  public static final String MESSAGE_GROUP_LOW_WATERMARK =
      "twister2.network.message.group.low_water_mark";
  public static final String MESSAGE_GROUP_HIGH_WATERMARK =
      "twister2.network.message.group.high_water_mark";

  public static final String MESSAGE_GROUPING_SIZE
      = "twister2.network.message.grouping.size";

  public static final String SHUFFLE_MAX_BYTES_IN_MEMORY =
      "twister2.network.shuffle.memory.bytes.max";
  public static final String SHUFFLE_MAX_RECORDS_IN_MEMORY =
      "twister2.network.shuffle.memory.records.max";
  public static final String SHUFFLE_MAX_FILE_SIZE =
      "twister2.network.shuffle.file.bytes.max";
  public static final String SHUFFLE_PARALLEL_IO =
      "twister2.network.shuffle.parallel.io";
  public static final String RING_GROUPING_WORKER_PER_GROUPS =
      "twister2.network.partition.ring.group.workers";

  public static final int DEFAULT_DESTINATION = 0;

  public static final List<Object> EMPTY_OBJECT = new ArrayList<>();

  /**
   * Twister2 has the support to use disk for some operations. For example,
   * keyed gather in batch mode can disk to handle tera bytes of data. This flag can be used
   * to signal engine to use disk whenever possible. Please note that, this option is
   * not applicable for all the operations and not intended to use directly.
   */
  public static final String USE_DISK = "use-disk";

  /**
   * Twister2 keyed operations sometimes requires to compare keys. This property
   * can be set to define key comparator for such operations.
   */
  public static final String KEY_COMPARATOR = "key-comparator";

  /**
   * Type of the join algorithm to apply with join operation
   */
  public static final String JOIN_TYPE = "join-type";
  public static final String JOIN_ALGORITHM = "join-algorithm";

  public enum JoinType {
    INNER, FULL_OUTER, LEFT, RIGHT;

    public boolean includeLeft() {
      return this == FULL_OUTER || this == LEFT || this == INNER;
    }

    public boolean includeRight() {
      return this == FULL_OUTER || this == RIGHT || this == INNER;
    }
  }

  public enum JoinAlgorithm {
    SORT, HASH
  }

  /**
   * Some keyed communication operations are capable of sorting the outputs by key. This
   * property can be used to configure such operators.
   */
  public static final String SORT_BY_KEY = "sort-by-key";

  public static final String GROUP_BY_KEY = "group-by-key";

  public static int interNodeDegree(Config cfg, int defaultValue) {
    return getIntPropertyValue(cfg, INTER_NODE_DEGREE, defaultValue);
  }

  public static int intraNodeDegree(Config cfg, int defaultValue) {
    return getIntPropertyValue(cfg, INTRA_NODE_DEGREE, defaultValue);
  }

  public static List<String> persistentDirectory(Config cfg) {
    return cfg.getListValue(PERSISTENT_DIRECTORIES,
        Collections.singletonList(PERSISTENT_DIRECTORY_DEFAULT_VALUE))
        .stream().map(dir -> TokenSub.substitute(cfg, dir, Context.substitutions))
        .collect(Collectors.toList());
  }

  public static String partitionAlgorithm(Config cfg) {
    return getStringPropertyValue(cfg, ALLTOALL_ALGO_KEY, ALLTOALL_ALGO_RING);
  }

  public static int bufferSize(Config cfg) {
    return getIntPropertyValue(cfg, BUFFER_SIZE, 2048000);
  }

  public static int sendBuffersCount(Config cfg) {
    return getIntPropertyValue(cfg, SEND_BUFFERS_COUNT, 32);
  }

  public static int receiveBufferCount(Config cfg) {
    return getIntPropertyValue(cfg, RECEIVE_BUFFERS_COUNT, 32);
  }

  public static int sendPendingMax(Config cfg) {
    return getIntPropertyValue(cfg, SEND_PENDING_MAX, 16);
  }

  public static int networkChannelPendingSize(Config cfg) {
    return getIntPropertyValue(cfg, CHANNEL_PENDING_SIZE, 1024);
  }

  public static int getNetworkPartitionMessageGroupLowWaterMark(Config cfg) {
    return getIntPropertyValue(cfg, MESSAGE_GROUP_LOW_WATERMARK, 8);
  }

  public static int getNetworkPartitionMessageGroupHighWaterMark(Config cfg) {
    return getIntPropertyValue(cfg, MESSAGE_GROUP_HIGH_WATERMARK, 16);
  }

  public static long getShuffleMaxRecordsInMemory(Config cfg) {
    return getLongPropertyValue(cfg, SHUFFLE_MAX_RECORDS_IN_MEMORY, 64L);
  }

  public static long getShuffleMaxBytesInMemory(Config cfg) {
    return getLongPropertyValue(cfg, SHUFFLE_MAX_BYTES_IN_MEMORY, 6400L);
  }

  public static int getNetworkPartitionBatchGroupingSize(Config cfg) {
    return getIntPropertyValue(cfg, MESSAGE_GROUPING_SIZE, 100);
  }

  public static long getShuffleFileSize(Config cfg) {
    return cfg.getLongValue(SHUFFLE_MAX_FILE_SIZE, 1000000);
  }

  public static int getParallelIOAllowance(Config cfg) {
    return getIntPropertyValue(cfg, SHUFFLE_PARALLEL_IO, 1);
  }

  public static int getRingWorkersPerGroup(Config cfg) {
    return getIntPropertyValue(cfg, RING_GROUPING_WORKER_PER_GROUPS, 128);
  }
}
