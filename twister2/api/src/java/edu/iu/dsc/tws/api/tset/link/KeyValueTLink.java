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

package edu.iu.dsc.tws.api.tset.link;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;

public abstract class KeyValueTLink<K, V> extends BaseTLink<Tuple<K, V>> {

  private PartitionFunction<K> partitionFunction;

  private Selector<K, V> selector;

  protected KeyValueTLink(TSetEnvironment tSetEnv, String name, int sourceParallelism,
                PartitionFunction<K> parFn, Selector<K, V> selec) {
    super(tSetEnv, name, sourceParallelism);
    this.partitionFunction = parFn;
    this.selector = selec;
  }

  Class<? super K> getClassK() {
    return new TypeToken<K>(getClass()) {
    }.getRawType();
  }

  Class<? super V> getClassV() {
    return new TypeToken<V>(getClass()) {
    }.getRawType();
  }


  public PartitionFunction<K> getPartitionFunction() {
    return partitionFunction;
  }

  public Selector<K, V> getSelector() {
    return selector;
  }
}
