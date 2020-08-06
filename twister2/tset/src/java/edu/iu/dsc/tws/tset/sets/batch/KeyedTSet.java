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


package edu.iu.dsc.tws.tset.sets.batch;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorToTupleOp;
import edu.iu.dsc.tws.tset.ops.ComputeToTupleOp;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class KeyedTSet<K, V> extends BatchTupleTSetImpl<K, V> {
  private TFunction<Tuple<K, V>, ?> mapToTupleFunc;

  private KeyedTSet() {
    //non arg constructor for kryo
  }

  /**
   * NOTE: KeyedTSets input usually comes from a non keyed
   * {@link edu.iu.dsc.tws.api.tset.link.TLink}. Hence the input schema is a not a
   * {@link KeyedSchema}
   */
  public KeyedTSet(BatchEnvironment tSetEnv, TFunction<Tuple<K, V>, ?> mapFunc,
                   int parallelism, Schema inputSchema) {
    super(tSetEnv, "keyed", parallelism, inputSchema);
    this.mapToTupleFunc = mapFunc;
  }

  @Override
  public KeyedTSet<K, V> setName(String n) {
    return (KeyedTSet<K, V>) super.setName(n);
  }

  @Override
  public KeyedTSet<K, V> withSchema(TupleSchema schema) {
    return (KeyedTSet<K, V>) super.withSchema(schema);
  }

  @Override
  public ICompute getINode() {

    if (mapToTupleFunc instanceof ComputeFunc) {
      return new ComputeToTupleOp<>((ComputeFunc<Tuple<K, V>, ?>) mapToTupleFunc, this,
          getInputs());
    } else if (mapToTupleFunc instanceof ComputeCollectorFunc) {
      return new ComputeCollectorToTupleOp<>((ComputeCollectorFunc<Tuple<K, V>, ?>) mapToTupleFunc,
          this, getInputs());
    }

    throw new RuntimeException("Unknown map function passed to keyed tset" + mapToTupleFunc);
  }
}
