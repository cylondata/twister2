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
package edu.iu.dsc.tws.api.tset.fn;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.api.tset.TSetContext;

public class MapIterCompute<O, I> implements ComputeCollectorFunc<O, Iterator<I>> {
  private MapFunc<O, I> mapFn;

  public MapIterCompute(MapFunc<O, I> mapFunction) {
    this.mapFn = mapFunction;
  }

  @Override
  public void compute(Iterator<I> input, Collector<O> output) {
    while (input.hasNext()) {
      O result = mapFn.map(input.next());
      output.collect(result);
    }
  }

  @Override
  public void prepare(TSetContext context) {
    mapFn.prepare(context);
  }
}
