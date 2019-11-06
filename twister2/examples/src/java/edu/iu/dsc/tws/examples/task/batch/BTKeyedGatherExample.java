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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.typed.batch.BKeyedGatherGroupedCompute;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;

public class BTKeyedGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTKeyedGatherExample.class.getName());

  @Override
  public ComputeGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    MessageType keyType = MessageTypes.INTEGER;
    MessageType dataType = MessageTypes.INTEGER_ARRAY;
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new KeyedGatherGroupedSinkTask();
    computeGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = computeGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedGather(SOURCE)
        .viaEdge(edge)
        .withKeyType(keyType)
        .withTaskPartitioner(new HashingPartitioner())
        .withDataType(dataType);
    return computeGraphBuilder;
  }

  protected static class KeyedGatherGroupedSinkTask
      extends BKeyedGatherGroupedCompute<Integer, int[]> implements ISink {

    private static final long serialVersionUID = -254264903510284798L;

    private ResultsVerifier<int[], Iterator<Tuple<Integer, Iterator<int[]>>>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
        List<Integer> sinkIndex = ctx.getTasksByName(SINK).stream()
            .map(TaskInstancePlan::getTaskIndex)
            .collect(Collectors.toList());

        sinkIndex.sort(Comparator.comparingInt(o -> o));

        int thisTaskIndex = Integer.parseInt(args.get("taskIndex").toString());

        Set<Integer> keysRoutedToThis = new HashSet<>();
        for (Integer i = 0; i < jobParameters.getTotalIterations(); i++) {
          if (sinkIndex.get(i.hashCode() % sinkIndex.size()) == thisTaskIndex) {
            keysRoutedToThis.add(i);
          }
        }

        List<int[]> dataForEachKey = new ArrayList<>();
        for (int i = 0; i < ctx.getTasksByName(SOURCE).size(); i++) {
          dataForEachKey.add(ints);
        }

        List<Tuple<Integer, Iterator<int[]>>> expectedData = new ArrayList<>();

        for (Integer key : keysRoutedToThis) {
          expectedData.add(new Tuple<>(key, dataForEachKey.iterator()));
        }

        return expectedData.iterator();
      }, new IteratorComparator<>(
          new TupleComparator<>(
              (d1, d2) -> true, //return true for any key, since we
              // can't determine this due to hash based selector
              new IteratorComparator<>(
                  IntArrayComparator.getInstance()
              )
          )
      ));
    }

    @Override
    public boolean keyedGather(Iterator<Tuple<Integer, Iterator<int[]>>> content) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
      LOG.info(String.format("%d received keyed-gather %d",
          context.getWorkerId(), context.globalTaskId()));
      BenchmarkUtils.markTotalTime(resultsRecorder, this.timingCondition);
      resultsRecorder.writeToCSV();
      this.verified = verifyResults(resultsVerifier, content,
          Collections.singletonMap("taskIndex", context.taskIndex()), verified);
      return true;
    }
  }
}
