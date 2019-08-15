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

package edu.iu.dsc.tws.api.tset.env;

import edu.iu.dsc.tws.api.compute.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;

public class BatchTSetEnvironment extends TSetEnvironment {

  public BatchTSetEnvironment(WorkerEnvironment wEnv) {
    super(wEnv);
  }

  @Override
  public OperationMode getOperationMode() {
    return OperationMode.BATCH;
  }

  @Override
  public <T> SourceTSet<T> createSource(SourceFunc<T> source, int parallelism) {
    SourceTSet<T> sourceT = new SourceTSet<>(this, source, parallelism);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  /**
   * Runs a subgraph of TSets from the specified TSet
   *
   * @param leafTset leaf tset
   */
  public void run(BaseTSet leafTset) {
    DataFlowTaskGraph dataflowGraph = getTSetGraph().build(leafTset);
    executeDataFlowGraph(dataflowGraph, null);
  }

  /**
   * Runs a subgraph of TSets from the specified TSet and output results as a tset
   *
   * @param leafTset leaf tset
   * @param <T> type of the output data object
   * @return output result as a data object
   */
  public <T> DataObject<T> runAndGet(BaseTSet leafTset) {
    DataFlowTaskGraph dataflowGraph = getTSetGraph().build(leafTset);
    return executeDataFlowGraph(dataflowGraph, leafTset);
  }
}
