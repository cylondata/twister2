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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.JoinedTuple;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.batch.BJoinCompute;

public class BTJoinExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTKeyedGatherExample.class.getName());
  private static final String RIGHT_EDGE = "right";
  private static final String LEFT_EDGE = "left";

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.INTEGER;
    DataType dataType = DataType.INTEGER_ARRAY;


    BaseSource g = new JoinSource();
    ISink r = new JoinSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.join(SOURCE, SOURCE)
        .viaLeftEdge(LEFT_EDGE)
        .viaRightEdge(RIGHT_EDGE)
        .withKeyType(keyType)
        .withLeftDataType(dataType).withRightDataType(dataType).withComparator(
            new Comparator<Integer>() {
              @Override
              public int compare(Integer o1, Integer o2) {
                return o1 - o2;
              }
            });
    return taskGraphBuilder;
  }

  protected static class JoinSinkTask
      extends BJoinCompute<Integer, int[], int[]> implements ISink {

    private static final long serialVersionUID = -254264903510284798L;

    private ResultsVerifier<int[], Iterator<Tuple<Integer, Iterator<int[]>>>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
        Set<Integer> taskIds = ctx.getTasksByName(SOURCE).stream()
            .map(TaskInstancePlan::getTaskIndex)
            .filter(i -> (Math.abs(i.hashCode())) == ctx.taskIndex())
            .collect(Collectors.toSet());

        List<int[]> dataFromEachTask = new ArrayList<>();
        for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
          dataFromEachTask.add(ints);
        }

        List<Tuple<Integer, Iterator<int[]>>> finalOutput = new ArrayList<>();

        taskIds.forEach(key -> {
          finalOutput.add(new Tuple<>(key, dataFromEachTask.iterator()));
        });

        return finalOutput.iterator();
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
    public boolean join(Iterator<JoinedTuple<Integer, int[], int[]>> content) {
      LOG.info("Received joined tuple");
      return true;
    }
  }

  protected static class JoinSource extends BaseSource {
    private int count = 0;

    private int iterations;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
    }

    @Override
    public void execute() {
      count++;
      if (count > iterations) {
        context.end(LEFT_EDGE);
        context.end(RIGHT_EDGE);
      }

      if (count % 2 == 1) {
        context.write(LEFT_EDGE, count, inputDataArray);
      } else {
        context.write(RIGHT_EDGE, count, inputDataArray);
      }
    }
  }

}
