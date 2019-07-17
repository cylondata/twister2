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

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunction;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.sets.ComputeCollectorTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableMapTSet;

public class DirectTLink<T> extends BaseTLink<T> {

  public DirectTLink(TSetEnvironment tSetEnv, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("direct"), sourceParallelism);
  }

  public <P> IterableMapTSet<T, P> map(IterableMapFunction<T, P> mapFn) {
    IterableMapTSet<T, P> set = new IterableMapTSet<>(getTSetEnv(), mapFn, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> IterableFlatMapTSet<T, P> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    IterableFlatMapTSet<T, P> set = new IterableFlatMapTSet<>(getTSetEnv(), mapFn,
        getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> ComputeTSet<Iterator<T>, P> compute(ComputeFunction<Iterator<T>, P> computeFunction) {
    ComputeTSet<Iterator<T>, P> set = new ComputeTSet<>(getTSetEnv(), computeFunction,
        getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> ComputeCollectorTSet<Iterator<T>, P> compute(ComputeCollectorFunction<Iterator<T>, P>
                                                              computeFunction) {
    ComputeCollectorTSet<Iterator<T>, P> set = new ComputeCollectorTSet<>(getTSetEnv(),
        computeFunction, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  @Override
  public void build(TSetGraph tSetGraph) {

  }

  @Override
  public DirectTLink<T> setName(String name) {
    rename(name);
    return this;
  }
}
