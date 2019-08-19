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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.api.tset.sources.HadoopSource;

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

  public <K, V, F extends InputFormat<K, V>> SourceTSet<Tuple<K, V>> createHadoopSource(
      Configuration configuration, Class<F> inputFormat) {
    JobConf jconf = new JobConf(configuration);
    InputFormat<K, V> format;
    try {
      FileSystem.getLocal(configuration);
      format = inputFormat.newInstance();
      JobContext jobContext = new JobContextImpl(configuration, new JobID("hadoop", 1));
      List<InputSplit> splits = format.getSplits(jobContext);
      return new SourceTSet<>(this, new HadoopSource<>(), splits.size());
    } catch (InstantiationException | IllegalAccessException
        | InterruptedException | IOException e) {
      throw new RuntimeException("Failed to initialize hadoop input", e);
    }
  }

  /**
   * Runs a subgraph of TSets from the specified TSet
   *
   * @param leafTset leaf tset
   */
  public void run(BaseTSet leafTset) {
    ComputeGraph dataflowGraph = getTSetGraph().build(leafTset);
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
    ComputeGraph dataflowGraph = getTSetGraph().build(leafTset);
    return executeDataFlowGraph(dataflowGraph, leafTset);
  }
}
