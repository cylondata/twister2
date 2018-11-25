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
package edu.iu.dsc.tws.api.tset.impl;

import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.ops.MapOp;

public class MapTSet<T, P> extends BaseTSet<T> {
  private BaseTSet<P> parent;

  private MapFunction<P, T> mapFn;

  public MapTSet(BaseTSet<P> parent, MapFunction<P, T> mapFunc) {
    this.parent = parent;
    this.mapFn = mapFunc;
  }

  protected void build() {
    builder.addCompute(name, new MapOp<>(mapFn), parallel);
  }
}
