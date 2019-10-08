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

package edu.iu.dsc.tws.tset.env;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;

public class StreamingTSetEnvironment extends TSetEnvironment {

  public StreamingTSetEnvironment(WorkerEnvironment wEnv) {
    super(wEnv);
  }

  public StreamingTSetEnvironment() {
    super();
  }

  @Override
  public OperationMode getOperationMode() {
    return OperationMode.STREAMING;
  }

  @Override
  public <T> SSourceTSet<T> createSource(SourceFunc<T> source, int parallelism) {
    SSourceTSet<T> sourceT = new SSourceTSet<>(this, source, parallelism);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  /**
   * Runs the entire TSet graph
   */
  public void run() {
    ComputeGraph graph = getTSetGraph().build().getComputeGraph();
    executeDataFlowGraph(graph, null, false);
  }

}
