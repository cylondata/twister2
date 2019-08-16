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
package edu.iu.dsc.tws.examples.task.streaming;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.compute.nodes.ISink;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.typed.streaming.SPartitionKeyedCompute;

public class STPartitionKeyedExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STPartitionKeyedExample.class.getName());

  @Override
  public ComputeGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    MessageType keyType = MessageTypes.INTEGER;
    MessageType dataType = MessageTypes.INTEGER_ARRAY;
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new SKeyedPartitionSinkTask();
    computeGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = computeGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedPartition(SOURCE)
        .viaEdge(edge)
        .withKeyType(keyType)
        .withDataType(dataType);
    return computeGraphBuilder;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class SKeyedPartitionSinkTask extends SPartitionKeyedCompute<Integer, int[]>
      implements ISink {

    private static final long serialVersionUID = -254264903510284798L;
    private ResultsVerifier<int[], Tuple<Integer, int[]>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    private int count = 0;

    private int lowestSource = -1;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      int sinksCount = ctx.getTasksByName(SINK).size();
      final Set<Integer> taskIds = ctx.getTasksByName(SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .filter(i -> i % sinksCount == ctx.taskIndex())
          .collect(Collectors.toSet());
      this.lowestSource = taskIds.stream().min(Comparator.comparingInt(i -> (Integer) i)).get();
      resultsVerifier = new ResultsVerifier<>(inputDataArray,
          (ints, args) -> new Tuple<>(0, ints),
          new TupleComparator<>(
              (d1, d2) -> taskIds.contains(d2), //accept any int from the set of eligible task ids
              IntArrayComparator.getInstance()
          ));

      receiversInProgress.incrementAndGet();
    }

    @Override
    public boolean keyedPartition(Tuple<Integer, int[]> data) {
      // this task can be the target for many sources, let's consider just the
      // messages from the lowest source for benchmark and verification
      if (data.getKey() == this.lowestSource) {
        count++;
        if (count > jobParameters.getWarmupIterations()) {
          Timing.mark(BenchmarkConstants.TIMING_MESSAGE_RECV, this.timingCondition);
        }

        if (count == jobParameters.getTotalIterations()) {
          LOG.info(String.format("%d received keyed-partition %d",
              context.getWorkerId(), context.globalTaskId()));
          Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, this.timingCondition);
          resultsRecorder.writeToCSV();
          receiversInProgress.decrementAndGet();
        }
        this.verified = verifyResults(resultsVerifier, data, null, verified);
      }
      return true;
    }
  }
}
