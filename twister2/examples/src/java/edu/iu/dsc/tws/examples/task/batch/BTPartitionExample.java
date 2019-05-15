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
package edu.iu.dsc.tws.examples.task.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.batch.BPartitionCompute;

public class BTPartitionExample extends BenchTaskWorker {
  private static final Logger LOG = Logger.getLogger(BTPartitionExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER_ARRAY;
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new PartitionSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.partition(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class PartitionSinkTask extends BPartitionCompute<int[]> implements ISink {
    private static final long serialVersionUID = -254264903510284798L;

    private ResultsVerifier<int[], Iterator<int[]>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);

      int totalSinks = ctx.getTasksByName(SINK).size();
      long noOfSources = ctx.getTasksByName(SOURCE).stream().map(
          TaskInstancePlan::getTaskIndex
      ).filter(ti -> ti % totalSinks == ctx.taskIndex()).count();

      if (jobParameters.getTotalIterations() % totalSinks != 0) {
        LOG.warning("Total iterations is not divisible by total sinks. "
            + "Verification won't run for this configuration.");
      } else {
        resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
          List<int[]> expectedData = new ArrayList<>();

          for (long i = 0; i < noOfSources * jobParameters.getTotalIterations(); i++) {
            expectedData.add(ints);
          }
          return expectedData.iterator();
        }, new IteratorComparator<>(
            IntArrayComparator.getInstance()
        ));
      }
    }

    @Override
    public boolean partition(Iterator<int[]> content) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
      LOG.info(String.format("%d received partition %d", context.getWorkerId(),
          context.globalTaskId()));
      BenchmarkUtils.markTotalTime(resultsRecorder, this.timingCondition);
      resultsRecorder.writeToCSV();
      if (resultsVerifier != null) {
        this.verified = verifyResults(resultsVerifier, content, null, verified);
      }
      return true;
    }
  }
}
