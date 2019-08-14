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

import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.api.task.nodes.ISink;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.typed.streaming.SPartitionCompute;

/**
 * todo add timing
 */
public class STPartitionExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STPartitionExample.class.getName());

  @Override
  public ComputeGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    MessageType dataType = MessageTypes.INTEGER_ARRAY;
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ((SourceTask) g).setMarkTimingOnlyForLowestTarget(true);
    ISink r = new PartitionSinkTask();
    computeGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = computeGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.partition(SOURCE).viaEdge(edge).withDataType(dataType);
    return computeGraphBuilder;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class PartitionSinkTask extends SPartitionCompute<int[]> implements ISink {

    private static final long serialVersionUID = -254264903510284798L;
    private ResultsVerifier<int[], int[]> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    private int count = 0;
    private int countTotal = 0;

    //expected counts from a single target
    private int expectedWarmups = 0;
    private int expectedTotal = 0;

    //expectedCounts from all targets
    private int expectedTotalFromAll = 0;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);

      resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> ints,
          IntArrayComparator.getInstance());
      receiversInProgress.incrementAndGet();

      int noOfSinks = ctx.getTasksByName(SINK).size();

      expectedWarmups = jobParameters.getWarmupIterations() / noOfSinks;
      if (jobParameters.getWarmupIterations() % noOfSinks > 0
          && jobParameters.getWarmupIterations() % noOfSinks > ctx.taskIndex()) {
        expectedWarmups++;
      }

      expectedTotal = jobParameters.getTotalIterations() / noOfSinks;
      if (jobParameters.getTotalIterations() % noOfSinks > 0
          && jobParameters.getWarmupIterations() % noOfSinks > ctx.taskIndex()) {
        expectedTotal++;
      }

      expectedTotalFromAll = expectedTotal * ctx.getTasksByName(SOURCE).size();
      LOG.info(String.format("%d expecting %d warmups and %d total",
          ctx.taskIndex(), expectedWarmups, expectedTotal));
    }

    @Override
    public boolean partition(int[] data) {
      //todo not taking timing
      countTotal++;
      if (countTotal == expectedTotalFromAll) {
        receiversInProgress.decrementAndGet();
      }
      this.verified = verifyResults(resultsVerifier, data, null, verified);
      return true;
    }
  }
}
