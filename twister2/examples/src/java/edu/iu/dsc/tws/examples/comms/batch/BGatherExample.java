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
import edu.iu.dsc.tws.comms.api.batch.BGather;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IntComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;

public class BGatherExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BGatherExample.class.getName());

  private BGather gather;

  private boolean gatherDone;
  private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;

  @Override
  protected void execute() {

    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;
    // create the communication
    gather = new BGather(communicator, taskPlan, sources, target,
        MessageType.INTEGER, new FinalReduceReceiver(), false);


    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
      gatherDone = true;
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> {
          List<Tuple<Integer, int[]>> expectedData = new ArrayList<>();
          for (Integer source : sources) {
            for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
              expectedData.add(new Tuple<>(source, ints));
            }
          }
          return expectedData.iterator();
        },
        new IteratorComparator<>(
            new TupleComparator<>(
                IntComparator.getInstance(),
                IntArrayComparator.getInstance()
            )
        ));

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, target, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    gather.close();
  }

  @Override
  protected void progressCommunication() {
    gather.progress();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!gather.gather(task, data, flag)) {
      // lets wait a litte and try again
      gather.progress();
    }
    return true;
  }

  @Override
  protected boolean isDone() {
    return gatherDone && sourcesDone && !gather.hasPending();
  }

  @Override
  protected void finishCommunication(int src) {
    gather.finish(src);
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
    public boolean receive(int target, Iterator<Object> itr) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV,
          workerId == 0 && target == lowestTarget);
      BenchmarkUtils.markTotalTime(resultsRecorder, workerId == 0
          && target == lowestTarget);
      resultsRecorder.writeToCSV();
      gatherDone = true;
      verifyResults(resultsVerifier, itr, null);
      return true;
    }
  }
}
