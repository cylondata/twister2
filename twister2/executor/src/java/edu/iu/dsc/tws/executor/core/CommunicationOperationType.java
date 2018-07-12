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
package edu.iu.dsc.tws.executor.core;

public final class CommunicationOperationType {

  private CommunicationOperationType() {
  }

  public static final String BATCH_REDUCE = "batch_reduce";
  public static final String BATCH_GATHER = "batch_gather";
  public static final String BATCH_PARTITION = "batch_partition";
  public static final String BATCH_ALLGATHER = "batch_allgather";
  public static final String BATCH_ALLREDUCE = "batch_allreduce";
  public static final String BATCH_BROADCAST = "batch_broadcast";
  public static final String STREAMING_REDUCE = "stream_reduce";
  public static final String STREAMING_GATHER = "stream_gather";
  public static final String STREAMING_KEYED_REDUCE = "stream_keyed_reduce";
  public static final String BATCH_KEYED_REDUCE = "batch_keyed_reduce";
  public static final String STREAMING_PARTITION = "stream_partition";
  public static final String STREAMING_ALLGATHER = "stream_allgather";
  public static final String STREAMING_ALLREDUCE = "stream_allreduce";
  public static final String STREAMING_BROADCAST = "stream_broadcast";
  public static final String BATCH_PARTITION_BY_MULTI_BYTE = "batch_parition_by_multi_byte";
  public static final String STREAMING_PARTITION_BY_MULTI_BYTE = "streaming_parition_by_multi_byte";

}

