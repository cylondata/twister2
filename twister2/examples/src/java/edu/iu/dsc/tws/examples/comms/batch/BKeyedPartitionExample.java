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
import edu.iu.dsc.tws.comms.api.batch.BKeyedPartition;
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

/**
 * This class is a example use of the keyed partition function
 */
public class BKeyedPartitionExample extends KeyedBenchWorker {

  private static final Logger LOG = Logger.getLogger(BKeyedPartitionExample.class.getName());

  private BKeyedPartition partition;

  private boolean partitionDone = false;
  private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Set<Integer> targets = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    // create the communication
    partition = new BKeyedPartition(communicator, taskPlan, sources, targets, MessageType.INTEGER,
        MessageType.INTEGER, new PartitionReceiver(), new SimpleKeyBasedSelector());

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    // now initialize the workers

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
      int lowestTarget = targets.stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
      int target = Integer.valueOf(args.get("target").toString());
      Set<Integer> keysRoutedToThis = new HashSet<>();
      for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
        if (i % targets.size() == target - lowestTarget) {
          keysRoutedToThis.add(i);
        }
      }

      List<Tuple<Integer, int[]>> expectedData = new ArrayList<>();

      for (Integer key : keysRoutedToThis) {
        for (int i = 0; i < sources.size(); i++) {
          expectedData.add(new Tuple<>(key, ints));
        }
      }

      return expectedData.iterator();
    }, new IteratorComparator<>(
        new TupleComparator<>(
            (d1, d2) -> true, //any int
            IntArrayComparator.getInstance()
        )
    ));

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new KeyedBenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    partition.close();
  }

  @Override
  protected void progressCommunication() {
    partition.progress();
  }

  @Override
  protected boolean isDone() {
    return partitionDone && sourcesDone && !partition.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!partition.partition(task, key, data, flag)) {
      // lets wait a litte and try again
      partition.progress();
    }
    return true;
  }

  public class PartitionReceiver implements BulkReceiver {
    private int lowestTarget = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      if (targets.isEmpty()) {
        partitionDone = true;
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
      partitionDone = true;
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    partition.finish(src);
  }
}
