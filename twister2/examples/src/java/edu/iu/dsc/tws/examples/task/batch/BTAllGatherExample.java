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
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleIteratorComparator;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.AllGatherCompute;

public class BTAllGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTAllGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    ISource g = new SourceTask(edge);
    ISink r = new AllGatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allgather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class AllGatherSinkTask extends AllGatherCompute<int[]> implements ISink {
    private static final long serialVersionUID = -254264903510284798L;

    private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      resultsVerifier = new ResultsVerifier<>(
          inputDataArray,
          (ints, args) -> {
            Set<Integer> taskIds = ctx.getTasksByName(SOURCE).stream()
                .map(TaskInstancePlan::getTaskIndex)
                .collect(Collectors.toSet());
            List<Tuple<Integer, int[]>> generatedData = new ArrayList<>();
            for (Integer taskIndex : taskIds) {
              for (int i = 0; i < jobParameters.getIterations(); i++) {
                generatedData.add(new Tuple<>(100000 + taskIndex, ints));
              }
            }
            return generatedData.iterator();
          },
          new TupleIteratorComparator<>(IntArrayComparator.getInstance())
      );
    }

    @Override
    public boolean allGather(Iterator<Tuple<Integer, int[]>> itr) {
      LOG.info(String.format("%d received gather %d", context.getWorkerId(), context.taskId()));
      resultsVerifier.verify(itr);
//      BenchmarkUtils.markTotalTime(resultsRecorder, context.getWorkerId() == 0);
//      resultsRecorder.writeToCSV();
//      verifyResults(resultsVerifier, itr, null);
      return true;
    }
  }
}
