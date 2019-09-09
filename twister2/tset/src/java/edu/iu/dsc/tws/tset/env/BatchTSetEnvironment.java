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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.sources.HadoopSource;
import edu.iu.dsc.tws.tset.sources.HadoopSourceWithMap;

public class BatchTSetEnvironment extends TSetEnvironment {

  private BaseTSet cachedLeaf;
  private ComputeGraph cachedGraph;

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

  public <K, V, F extends InputFormat<K, V>> SourceTSet<Tuple<K, V>> createHadoopSource(
      Configuration configuration, Class<F> inputFormat, int parallel) {
    SourceTSet<Tuple<K, V>> sourceT = new SourceTSet<>(this,
        new HadoopSource<>(configuration, inputFormat), parallel);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  public <K, V, F extends InputFormat<K, V>, I> SourceTSet<I> createHadoopSource(
      Configuration configuration, Class<F> inputFormat, int parallel,
      MapFunc<I, Tuple<K, V>> mapFunc) {
    SourceTSet<I> sourceT = new SourceTSet<>(this,
        new HadoopSourceWithMap<>(configuration, inputFormat, mapFunc), parallel);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  /**
   * Runs a subgraph of TSets from the specified TSet
   *
   * @param leafTset leaf tset
   */
  public void run(BaseTSet leafTset) {
    ComputeGraph dataflowGraph = getTSetGraph().build(leafTset);
    executeDataFlowGraph(dataflowGraph, null, false);
  }

  /**
   * Runs a subgraph of TSets from the specified TSet and output results as a tset
   *
   * @param leafTset leaf tset
   * @param <T> type of the output data object
   * @return output result as a data object
   */
  public <T> DataObject<T> runAndGet(BaseTSet leafTset, boolean isIterative) {
    ComputeGraph dataflowGraph;
    if (isIterative && cachedLeaf != null && cachedLeaf == leafTset) {
      dataflowGraph = cachedGraph;
    } else {
      dataflowGraph = getTSetGraph().build(leafTset);
      cachedGraph = dataflowGraph;
      cachedLeaf = leafTset;
    }
    return executeDataFlowGraph(dataflowGraph, leafTset, isIterative);
  }

  public <T> DataObject<T> runAndGet(BaseTSet leafTset) {
    return runAndGet(leafTset, false);
  }
}
