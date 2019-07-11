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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.TaskPartitioner;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.api.task.nodes.ISink;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.comms.utils.JoinUtils;
import edu.iu.dsc.tws.comms.utils.KeyComparatorWrapper;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.typed.batch.BJoinCompute;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;

public class BTJoinExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTJoinExample.class.getName());
  private static final String RIGHT_EDGE = "right";
  private static final String LEFT_EDGE = "left";

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    MessageType keyType = MessageTypes.INTEGER;
    MessageType dataType = MessageTypes.INTEGER_ARRAY;


    BaseSource g = new JoinSource();
    ISink r = new JoinSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.join(SOURCE, SOURCE)
        .viaLeftEdge(LEFT_EDGE)
        .viaRightEdge(RIGHT_EDGE)
        .withKeyType(keyType)
        .withLeftDataType(dataType).withRightDataType(dataType)
        .withTaskPartitioner(new TaskPartitioner() {
          private List<Integer> dst;

          @Override
          public void prepare(Set sources, Set destinations) {
            this.dst = new ArrayList<>(destinations);
            Collections.sort(this.dst);
          }

          @Override
          public int partition(int source, Object data) {
            return dst.get((Integer) data % dst.size());
          }

          @Override
          public void commit(int source, int partition) {

          }
        })
        .withComparator(Integer::compareTo);
    return taskGraphBuilder;
  }

  protected static class JoinSinkTask
      extends BJoinCompute<Integer, int[], int[]> implements ISink {

    private static final long serialVersionUID = -254264903510284798L;

    private ResultsVerifier<int[], Iterator<JoinedTuple>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
        List<Integer> sinkIds = ctx.getTasksByName(SINK).stream()
            .map(TaskInstancePlan::getTaskIndex)
            .sorted()
            .collect(Collectors.toList());

        long sources = ctx.getTasksByName(SOURCE).stream()
            .map(TaskInstancePlan::getTaskIndex)
            .count();

        List<Tuple> onLeftEdge = new ArrayList<>();
        List<Tuple> onRightEdge = new ArrayList<>();

        int iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
        for (int i = 0; i < sources; i++) {
          for (int key = 0; key < iterations; key++) {
            if (sinkIds.get(key % sinkIds.size()) == ctx.taskIndex()) {
              onLeftEdge.add(Tuple.of(key, inputDataArray));
            }
            if (sinkIds.get((key / 2) % sinkIds.size()) == ctx.taskIndex()) {
              onRightEdge.add(Tuple.of(key / 2, inputDataArray));
            }
          }
        }

        List objects = JoinUtils.innerJoin(onLeftEdge, onRightEdge,
            new KeyComparatorWrapper(Comparator.naturalOrder()));

        return (Iterator<JoinedTuple>) objects.iterator();
      }, new IteratorComparator<>(
          (d1, d2) -> d1.getKey().equals(d2.getKey())
      ));
    }

    @Override
    public boolean join(Iterator<JoinedTuple<Integer, int[], int[]>> content) {
      LOG.info("Received joined tuple");
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
      BenchmarkUtils.markTotalTime(resultsRecorder, this.timingCondition);
      resultsRecorder.writeToCSV();
      this.verified = verifyResults(resultsVerifier, content, null, verified);
      return true;
    }
  }

  protected static class JoinSource extends BaseSource {
    private int count = 0;

    private int iterations;

    private boolean timingCondition;
    private boolean endNotified;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
      this.timingCondition = getTimingCondition(SOURCE, ctx);
      sendersInProgress.incrementAndGet();
    }

    private void notifyEnd() {
      if (endNotified) {
        return;
      }
      sendersInProgress.decrementAndGet();
      endNotified = true;
      LOG.info(String.format("Source : %d done sending.", context.taskIndex()));
    }

    @Override
    public void execute() {
      if (count < iterations) {
        if (count == jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_ALL_SEND, this.timingCondition);
        }
        context.write(LEFT_EDGE, count, inputDataArray);
        context.write(RIGHT_EDGE, count / 2, inputDataArray);
        count++;
      } else if (!this.endNotified) {
        context.end(LEFT_EDGE);
        context.end(RIGHT_EDGE);
        this.notifyEnd();
      }
    }
  }
}
