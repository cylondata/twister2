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
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.GatherMapCompute;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.ops.MapToTupleIterOp;
import edu.iu.dsc.tws.tset.ops.MapToTupleOp;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class KeyedTSet<K, V> extends BatchTupleTSetImpl<K, V> {
  private TFunction<Tuple<K, V>, ?> mapToTupleFunc;

  public KeyedTSet(BatchTSetEnvironment tSetEnv, TFunction<Tuple<K, V>, ?> mapFunc,
                   int parallelism) {
    super(tSetEnv, "keyed", parallelism);
    this.mapToTupleFunc = mapFunc;
  }

  @Override
  public KeyedTSet<K, V> setName(String n) {
    return (KeyedTSet<K, V>) super.setName(n);
  }

  @Override
  public ICompute getINode() {

    if (mapToTupleFunc instanceof MapCompute) {
      return new MapToTupleOp<>((MapCompute<Tuple<K, V>, ?>) mapToTupleFunc, this,
          getInputs());
    } else if (mapToTupleFunc instanceof MapIterCompute
        || mapToTupleFunc instanceof GatherMapCompute) {
      return new MapToTupleIterOp<>((MapIterCompute<Tuple<K, V>, ?>) mapToTupleFunc, this,
          getInputs());
    }

    throw new RuntimeException("Unknown map function passed to keyed tset" + mapToTupleFunc);
  }
}
