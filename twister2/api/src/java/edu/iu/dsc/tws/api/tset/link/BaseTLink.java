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

import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;

public class BaseTLink<T> implements TLink<T> {
  @Override
  public TLink<T> LinkParallelism(int parallelism) {
    return null;
  }

  @Override
  public TLink<T> LinkName(String name) {
    return null;
  }

  @Override
  public TLink<T> direct() {
    return null;
  }

  @Override
  public TLink<T> reduce(ReduceFunction<T> reduceFn) {
    return null;
  }

  @Override
  public TLink<T> allReduce(ReduceFunction<T> reduceFn) {
    return null;
  }

  @Override
  public TLink<T> partition(PartitionFunction<T> partitionFn) {
    return null;
  }

  @Override
  public TLink<T> gather() {
    return null;
  }

  @Override
  public TLink<T> allGather() {
    return null;
  }
}
