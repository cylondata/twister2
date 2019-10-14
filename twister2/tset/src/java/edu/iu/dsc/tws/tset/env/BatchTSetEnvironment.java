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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.TSetUtils;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.sources.HadoopSource;
import edu.iu.dsc.tws.tset.sources.HadoopSourceWithMap;

public class BatchTSetEnvironment extends TSetEnvironment {
  private static final Logger LOG = Logger.getLogger(BatchTSetEnvironment.class.getName());

//  private BaseTSet cachedLeaf;
//  private ComputeGraph cachedGraph;

  // todo: make this fault tolerant. May be we can cache the buildContext along with the compute
  //  graphs and make build contexts serializable. So, that the state of these can be preserved.
  private Map<String, BuildContext> buildCtxCache = new HashMap<>();

  public BatchTSetEnvironment(WorkerEnvironment wEnv) {
    super(wEnv);
  }

  public BatchTSetEnvironment() {
    super();
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

  @Override
  public <T> SourceTSet<T> createSource(String name, SourceFunc<T> source, int parallelism) {
    SourceTSet<T> sourceT = new SourceTSet<>(this, name, source, parallelism);
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
    BuildContext buildContext = getTSetGraph().build(leafTset);
    executeBuildContext(buildContext, null);
  }

  /**
   * Runs a subgraph of TSets from the specified TSet and output results as a tset
   *
   * @param leafTset leaf tset
   * @param <T> type of the output data object
   * @return output result as a data object
   */
  public <T> DataObject<T> runAndGet(BaseTSet leafTset) {
//    ComputeGraph dataflowGraph;
//    if (isIterative && cachedLeaf != null && cachedLeaf == leafTset) {
//      dataflowGraph = cachedGraph;
//    } else {
//      TBaseBuildContext buildContext = getTSetGraph().build(leafTset);
//      dataflowGraph = buildContext.getComputeGraph();
//      cachedGraph = dataflowGraph;
//      cachedLeaf = leafTset;
//    }
    BuildContext buildContext = getTSetGraph().build(leafTset);
    return executeBuildContext(buildContext, leafTset);
  }

  // adds the data into the task executor
  public void addData(String key, DataObject data) {
    getTaskExecutor().addInput(key, data);
  }

  public <T> DataObject<T> getData(String key) {
    return getTaskExecutor().getOutput(key);
  }

  public boolean isDataAvailable(String key) {
    return getTaskExecutor().isOutputAvailable(key);
  }

  public void eval(BaseTSet leafTSet) {
    BuildContext buildCtx;
    String buildId = TSetUtils.generateBuildId(leafTSet);
    if (buildCtxCache.containsKey(buildId)) {
      buildCtx = buildCtxCache.get(buildId);
    } else {
      buildCtx = getTSetGraph().build(leafTSet);
      buildCtxCache.put(buildId, buildCtx);
    }

    // build the context which will create compute graph and execution plan
    buildCtx.build(getTaskExecutor());

    LOG.fine(buildCtx.getComputeGraph()::toString);
    LOG.fine(() -> "edges: " + buildCtx.getComputeGraph().getDirectedEdgesSet());
    LOG.fine(() -> "vertices: " + buildCtx.getComputeGraph().getTaskVertexSet());

    getTaskExecutor().itrExecute(buildCtx.getComputeGraph(), buildCtx.getExecutionPlan());
  }

  public void finishEval(BaseTSet leafTset) {
    BuildContext ctx = buildCtxCache.remove(TSetUtils.generateBuildId(leafTset));
    getTaskExecutor().closeExecution(ctx.getComputeGraph(), ctx.getExecutionPlan());
  }

  public <T> DataObject<T> evaluateAndGet(BaseTSet leafTset) {
    return null;
  }

//  public <T> DataObject<T> runAndGet(BaseTSet leafTset) {
//    return runAndGet(leafTset, false);
//  }
}
