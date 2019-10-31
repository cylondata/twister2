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
package edu.iu.dsc.tws.examples.comms.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BKeyedGather;
import edu.iu.dsc.tws.comms.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;

public class BKeyedGatherExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(BKeyedGatherExample.class.getName());

  private BKeyedGather keyedGather;

  private ResultsVerifier<int[], Iterator<Tuple<Integer, Iterator<int[]>>>> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    );

    // create the communication
    keyedGather = new BKeyedGather(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER, MessageTypes.INTEGER_ARRAY, new FinalReduceReceiver(),
        new SimpleKeyBasedSelector());

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();

    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
      int lowestTarget = logicalPlanBuilder.getTargets().
          stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
      int target = Integer.valueOf(args.get("target").toString());
      Set<Integer> keysRoutedToThis = new HashSet<>();
      for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
        if (i % logicalPlanBuilder.getTargets().size() == target - lowestTarget) {
          keysRoutedToThis.add(i);
        }
      }

      List<int[]> dataForEachKey = new ArrayList<>();
      for (int i = 0; i < logicalPlanBuilder.getSources().size(); i++) {
        dataForEachKey.add(ints);
      }

      List<Tuple<Integer, Iterator<int[]>>> expectedData = new ArrayList<>();

      for (Integer key : keysRoutedToThis) {
        expectedData.add(new Tuple<>(key, dataForEachKey.iterator()));
      }

      return expectedData.iterator();
    }, new IteratorComparator<>(
        new TupleComparator<>(
            (d1, d2) -> true, //any int
            new IteratorComparator<>(
                IntArrayComparator.getInstance()
            )
        )
    ));

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, logicalPlanBuilder.getSources(), 1, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    keyedGather.close();
  }

  @Override
  protected boolean progressCommunication() {
    keyedGather.progress();
    return !keyedGather.isComplete();
  }

  @Override
  protected boolean isDone() {
    boolean b = sourcesDone && keyedGather.isComplete();
    if (b) {
      LOG.info(String.format("%d is done", workerId));
    }
    return b;
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!keyedGather.gather(task, key, data, flag)) {
      // lets wait a litte and try again
      keyedGather.progress();
    }
    return true;
  }

  @Override
  protected void finishCommunication(int src) {
    keyedGather.finish(src);
  }

  public class FinalReduceReceiver implements BulkReceiver {
    private int lowestTarget = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      if (targets.isEmpty()) {
        return;
      }
      this.lowestTarget = targets.stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
    }

    @Override
    public boolean receive(int target, Iterator<Object> object) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV,
          workerId == 0 && target == lowestTarget);
      BenchmarkUtils.markTotalTime(resultsRecorder, workerId == 0
          && target == lowestTarget);
      resultsRecorder.writeToCSV();
      verifyResults(resultsVerifier, object, Collections.singletonMap("target", target));
      return true;
    }
  }
}
