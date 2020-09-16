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
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.TSetUtils;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.fn.GatherMapCompute;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorToTupleOp;
import edu.iu.dsc.tws.tset.ops.ComputeToTupleOp;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class SKeyedTSet<K, V> extends StreamingTupleTSetImpl<K, V> {
  private TFunction<?, Tuple<K, V>> mapToTupleFunc;

  public SKeyedTSet(StreamingEnvironment tSetEnv, String name, TFunction<?, Tuple<K, V>> mapFn,
                    int parallelism, Schema inputSchema) {
    super(tSetEnv, TSetUtils.resolveComputeName(name, mapFn, true, true),
        parallelism, inputSchema);
    this.mapToTupleFunc = mapFn;
  }

  @Override
  public SKeyedTSet<K, V> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public SKeyedTSet<K, V> withSchema(TupleSchema schema) {
    return (SKeyedTSet<K, V>) super.withSchema(schema);
  }

  @Override
  public ICompute getINode() {

    if (mapToTupleFunc instanceof MapCompute) {
      return new ComputeToTupleOp<>((MapCompute<?, Tuple<K, V>>) mapToTupleFunc, this,
          Collections.emptyMap());
    } else if (mapToTupleFunc instanceof MapIterCompute) {
      return new ComputeCollectorToTupleOp<>((MapIterCompute<?, Tuple<K, V>>) mapToTupleFunc, this,
          Collections.emptyMap());
    } else if (mapToTupleFunc instanceof GatherMapCompute) {
      return new ComputeCollectorToTupleOp<>((GatherMapCompute<?, Tuple<K, V>>) mapToTupleFunc,
          this, Collections.emptyMap());
    }

    throw new RuntimeException("Unknown map function passed to keyed tset" + mapToTupleFunc);
  }
}
