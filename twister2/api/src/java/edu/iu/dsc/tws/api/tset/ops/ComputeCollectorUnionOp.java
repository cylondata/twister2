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
package edu.iu.dsc.tws.api.tset.ops;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;

/**
 * This is now deprecated. The functionality is inbuilt to tset ops
 * @deprecated deprecated
 * @param <O>
 * @param <I>
 */
@Deprecated
public class ComputeCollectorUnionOp<O, I> extends ComputeCollectorOp<O, Iterator<I>> {

  private final int unionSize;
  private int completedCount;

  public ComputeCollectorUnionOp(ComputeCollectorFunc<O, Iterator<I>> computeFunction,
                                 int unionSize) {
    super(computeFunction);
    this.completedCount = 0;
    this.unionSize = unionSize;
  }

  @Override
  public void writeEndToEdges() {
    completedCount++;
    if (completedCount == unionSize) {
      super.writeEndToEdges();
    }
  }
}
