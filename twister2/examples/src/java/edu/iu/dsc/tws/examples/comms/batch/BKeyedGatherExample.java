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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BKeyedGather;
import edu.iu.dsc.tws.comms.api.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.Utils;
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

  private boolean gatherDone;
  private ResultsVerifier<int[], Iterator<Tuple<Integer, Iterator<int[]>>>> resultsVerifier;

  @Override
  protected void execute() {

    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Set<Integer> targets = new HashSet<>();
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }
    // create the communication
    keyedGather = new BKeyedGather(communicator, taskPlan, sources, targets,
        MessageType.INTEGER, MessageType.INTEGER, new FinalReduceReceiver(),
        new SimpleKeyBasedSelector());

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);

    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    gatherDone = true;
    for (int target : targets) {
      if (taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
        gatherDone = false;
      }
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
      int lowestTarget = targets.stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
      int target = Integer.valueOf(args.get("target").toString());
      Set<Integer> keysRoutedToThis = new HashSet<>();
      for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
        if (i % targets.size() == target - lowestTarget) {
          keysRoutedToThis.add(i);
        }
      }

      List<int[]> dataForEachKey = new ArrayList<>();
      for (int i = 0; i < sources.size(); i++) {
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
        workerId, sources, 1, tasksOfExecutor));
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
  protected void progressCommunication() {
    keyedGather.progress();
  }

  @Override
  protected boolean isDone() {
    boolean b = gatherDone && sourcesDone && !keyedGather.hasPending();
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
        gatherDone = true;
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
      gatherDone = true;
      return true;
    }
  }
}
