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

import java.util.Collections;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.ops.KeyedSourceOp;

public class SKeyedSourceTSet<K, V> extends StreamingTupleTSetImpl<K, V> {
  private SourceFunc<Tuple<K, V>> source;

  public SKeyedSourceTSet(StreamingEnvironment tSetEnv, SourceFunc<Tuple<K, V>> src,
                          int parallelism) {
    super(tSetEnv, "sksource", parallelism, PrimitiveSchemas.NULL_TUPLE2);
    this.source = src;
  }

  public SKeyedSourceTSet(StreamingEnvironment tSetEnv, String name,
                          SourceFunc<Tuple<K, V>> src, int parallelism) {
    super(tSetEnv, name, parallelism, PrimitiveSchemas.NULL_TUPLE2);
    this.source = src;
  }

  @Override
  public SKeyedSourceTSet<K, V> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public SKeyedSourceTSet<K, V> withSchema(TupleSchema schema) {
    return (SKeyedSourceTSet<K, V>) super.withSchema(schema);
  }

  @Override
  public INode getINode() {
    return new KeyedSourceOp<>(source, this, Collections.emptyMap());
  }
}
