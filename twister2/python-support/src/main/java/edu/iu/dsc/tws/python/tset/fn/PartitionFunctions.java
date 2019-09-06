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
package edu.iu.dsc.tws.python.tset.fn;

import java.util.Set;

import edu.iu.dsc.tws.api.tset.fn.LoadBalancePartitioner;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.python.processors.PythonClassProcessor;

public final class PartitionFunctions extends TFunc<PartitionFunc> {

  private static final PartitionFunctions INSTANCE = new PartitionFunctions();

  private PartitionFunctions() {

  }

  static PartitionFunctions getInstance() {
    return INSTANCE;
  }

  public LoadBalancePartitioner loadBalanced() {
    return new LoadBalancePartitioner();
  }

  /**
   * This method builds a partition function
   *
   * @param pyBinary serialized python class
   */
  @Override
  public PartitionFunc build(byte[] pyBinary) {
    PythonClassProcessor partitionPython = new PythonClassProcessor(pyBinary);

    return new PartitionFunc() {
      @Override
      public void prepare(Set sources, Set destinations) {
        partitionPython.invoke("prepare", sources, destinations);
      }

      @Override
      public int partition(int sourceIndex, Object val) {
        return (Integer) partitionPython.invoke("partition", sourceIndex, val);
      }

      @Override
      public void commit(int source, int partition) {
        partitionPython.invoke("commit", source, partition);
      }
    };
  }
}
