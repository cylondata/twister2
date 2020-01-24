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

import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingTupleTSet;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.links.streaming.SKeyedDirectTLink;
import edu.iu.dsc.tws.tset.links.streaming.SKeyedPartitionTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public abstract class StreamingTupleTSetImpl<K, V> extends BaseTSetWithSchema<V> implements
    StreamingTupleTSet<K, V> {

  StreamingTupleTSetImpl(StreamingTSetEnvironment tSetEnv, String name, int parallelism,
                         Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema, PrimitiveSchemas.OBJECT_TUPLE2);
  }

  @Override
  public StreamingTSetEnvironment getTSetEnv() {
    return (StreamingTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public SKeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    SKeyedPartitionTLink<K, V> partition = new SKeyedPartitionTLink<>(getTSetEnv(), partitionFn,
        getParallelism(), getOutputSchema());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public SKeyedDirectTLink<K, V> keyedDirect() {
    SKeyedDirectTLink<K, V> direct = new SKeyedDirectTLink<>(getTSetEnv(), getParallelism(),
        getOutputSchema());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public StreamingTupleTSetImpl<K, V> withSchema(TupleSchema schema) {
    this.setOutputSchema(schema);
    return this;
  }

  @Override
  public KeyedSchema getOutputSchema() {
    return (KeyedSchema) super.getOutputSchema();
  }
}
