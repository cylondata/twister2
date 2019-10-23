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
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.KeyedSourceOp;
import edu.iu.dsc.tws.tset.ops.SourceOp;

public class KeyedSourceTSet<K, V> extends BatchTupleTSetImpl<K, V> {
  private SourceFunc<Tuple<K, V>> source;

  public KeyedSourceTSet(BatchTSetEnvironment tSetEnv, SourceFunc<Tuple<K, V>> src,
                         int parallelism) {
    this(tSetEnv, "ksource", src, parallelism);
  }

  public KeyedSourceTSet(BatchTSetEnvironment tSetEnv, String name, SourceFunc<Tuple<K, V>> src,
                         int parallelism) {
    super(tSetEnv, name, parallelism);
    this.source = src;
  }

  @Override
  public KeyedSourceTSet<K, V> addInput(String key, Storable<?> input) {
    getTSetEnv().addInput(getId(), input.getId(), key);
    return this;
  }

  @Override
  public INode getINode() {
    return new KeyedSourceOp<>(source, this, getInputs());
  }

  @Override
  public KeyedSourceTSet<K, V> setName(String name) {
    rename(name);
    return this;
  }
}
