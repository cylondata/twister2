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

package edu.iu.dsc.tws.tset.sets.streaming;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingTupleTSet;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.links.streaming.SKeyedDirectTLink;
import edu.iu.dsc.tws.tset.links.streaming.SKeyedPartitionTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public abstract class StreamingTupleTSetImpl<K, V> extends BaseTSet<V> implements
    StreamingTupleTSet<K, V> {
  private MessageType kType = MessageTypes.OBJECT;
  private MessageType dType = MessageTypes.OBJECT;

  StreamingTupleTSetImpl(StreamingTSetEnvironment tSetEnv, String name, int parallelism) {
    super(tSetEnv, name, parallelism);
  }

  @Override
  public StreamingTSetEnvironment getTSetEnv() {
    return (StreamingTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public SKeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    SKeyedPartitionTLink<K, V> partition = new SKeyedPartitionTLink<>(getTSetEnv(), partitionFn,
        getParallelism(), getKeyType(), getDataType());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public SKeyedDirectTLink<K, V> keyedDirect() {
    SKeyedDirectTLink<K, V> direct = new SKeyedDirectTLink<>(getTSetEnv(), getParallelism(),
        getKeyType(), getDataType());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public StreamingTupleTSetImpl<K, V> withDataType(MessageType dataType) {
    this.dType = dataType;
    return this;
  }

  protected MessageType getDataType() {
    return this.dType;
  }

  @Override
  public StreamingTupleTSetImpl<K, V> withKeyType(MessageType keyType) {
    this.kType = keyType;
    return this;
  }

  protected MessageType getKeyType() {
    return this.kType;
  }
}
