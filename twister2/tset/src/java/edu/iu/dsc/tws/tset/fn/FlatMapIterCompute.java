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

package edu.iu.dsc.tws.tset.fn;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;

public class FlatMapIterCompute<O, I> implements ComputeCollectorFunc<O, Iterator<I>> {
  private FlatMapFunc<O, I> mapFn;

  public FlatMapIterCompute(FlatMapFunc<O, I> mapFunction) {
    this.mapFn = mapFunction;
  }

  @Override
  public void compute(Iterator<I> input, RecordCollector<O> output) {
    while (input.hasNext()) {
      mapFn.flatMap(input.next(), output);
    }
  }

  @Override
  public void prepare(TSetContext context) {
    mapFn.prepare(context);
  }
}
