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
package edu.iu.dsc.tws.comms.api;

import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;

public class CommunicationContext extends Context {
  private static final String INTER_NODE_DEGREE = "network.routing.inter.node.degree";
  private static final String INTRA_NODE_DEGREE = "network.routing.intra.node.degree";
  public static final ByteOrder DEFAULT_BYTEORDER = ByteOrder.BIG_ENDIAN;
  public static final String COMMUNICATION_TYPE = "network.type";
  public static final String MPI_COMMUNICATION_TYPE = "mpi";
  public static final String TCP_COMMUNICATION_TYPE = "tcp";
  public static final String PERSISTENT_DIRECTORIES = "network.ops.persistent.dirs";
  public static final String PERSISTENT_DIRECTORY_DEFAULT_VALUE = "${TWISTER2_HOME}/persistent/";

  public static final String TWISTER2_STREAM_KEYED_REDUCE_OP = "twister2.stream.keyed.reduce.op";
  public static final String TWISTER2_BATCH_KEYED_REDUCE_OP = "twister2.batch.keyed.reduce.op";
  public static final String TWISTER2_KEYED_REDUCE_OP_PARTITION = "partition";
  public static final String TWISTER2_KEYED_REDUCE_OP_REDUCE = "reduce";

  public static final String TWISTER2_STREAM_KEYED_GATHER_OP = "twister2.stream.keyed.gather.op";
  public static final String TWISTER2_BATCH_KEYED_GATHER_OP = "twister2.batch.keyed.gather.op";
  public static final String TWISTER2_KEYED_GATHER_OP_PARTITION = "partition";
  public static final String TWISTER2_KEYED_GATHER_OP_GATHER = "gather";

  public static final String TWISTER2_STREAM_PARTITION_ALGO_KEY =
      "twister2.stream.partition.algorithm";
  public static final String TWISTER2_BATCH_PARTITION_ALGO_KEY =
      "twister2.batch.partition.algorithm";
  public static final String TWISTER2_PARTITION_ALGO_SIMPLE = "simple";
  public static final String TWISTER2_PARTITION_ALGO_RING = "ring";

  public static int interNodeDegree(Config cfg, int defaultValue) {
    return cfg.getIntegerValue(INTER_NODE_DEGREE, defaultValue);
  }

  public static int intraNodeDegree(Config cfg, int defaultValue) {
    return cfg.getIntegerValue(INTRA_NODE_DEGREE, defaultValue);
  }

  public static String communicationType(Config cfg) {
    return cfg.getStringValue(COMMUNICATION_TYPE, MPI_COMMUNICATION_TYPE);
  }

  public static List<String> persistentDirectory(Config cfg) {
    return cfg.getListValue(PERSISTENT_DIRECTORIES,
        Collections.singletonList(PERSISTENT_DIRECTORY_DEFAULT_VALUE))
        .stream().map(dir -> TokenSub.substitute(cfg, dir, Context.substitutions))
        .collect(Collectors.toList());
  }

  public static String streamKeyedReduceOp(Config cfg) {
    return cfg.getStringValue(TWISTER2_STREAM_KEYED_REDUCE_OP, TWISTER2_KEYED_REDUCE_OP_PARTITION);
  }

  public static String batchKeyedReduceOp(Config cfg) {
    return cfg.getStringValue(TWISTER2_BATCH_KEYED_REDUCE_OP, TWISTER2_KEYED_REDUCE_OP_PARTITION);
  }

  public static String batchKeyedGatherOp(Config cfg) {
    return cfg.getStringValue(TWISTER2_BATCH_KEYED_GATHER_OP, TWISTER2_KEYED_GATHER_OP_PARTITION);
  }

  public static String streamKeyedGatherOp(Config cfg) {
    return cfg.getStringValue(TWISTER2_STREAM_KEYED_GATHER_OP, TWISTER2_KEYED_GATHER_OP_PARTITION);
  }

  public static String partitionStreamAlgorithm(Config cfg) {
    return cfg.getStringValue(TWISTER2_STREAM_PARTITION_ALGO_KEY, TWISTER2_PARTITION_ALGO_RING);
  }

  public static String partitionBatchAlgorithm(Config cfg) {
    return cfg.getStringValue(TWISTER2_BATCH_PARTITION_ALGO_KEY, TWISTER2_PARTITION_ALGO_RING);
  }
}
