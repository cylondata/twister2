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

import java.util.Set;

import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.task.api.TaskPartitioner;

public class TaskPartitionFunction<T> implements TaskPartitioner {
  private PartitionFunction<T> partitionFunction;

  public TaskPartitionFunction(PartitionFunction<T> parFn) {
    this.partitionFunction = parFn;
  }

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    partitionFunction.prepare(sources, destinations);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int partition(int source, Object data) {
    return partitionFunction.partition(source, (T) data);
  }

  @Override
  public void commit(int source, int partition) {
    partitionFunction.commit(source, partition);
  }
}
