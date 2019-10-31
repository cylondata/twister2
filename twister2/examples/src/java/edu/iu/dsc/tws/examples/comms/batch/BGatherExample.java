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
import edu.iu.dsc.tws.comms.batch.BGather;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
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

  private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    if (jobParameters.getTargets() != 1) {
      LOG.warning("Setting no of targets to 1. Found " + jobParameters.getTargets());
      jobParameters.getTaskStages().set(1, 1);
    }

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    );

    // create the communication
    gather = new BGather(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new FinalReduceReceiver(), false);


    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> {
          List<Tuple<Integer, int[]>> expectedData = new ArrayList<>();
          for (Integer source : logicalPlanBuilder.getSources()) {
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
        workerId, logicalPlanBuilder.getSources(),
        logicalPlanBuilder.getTargets().iterator().next(), tasksOfExecutor));
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
  protected boolean progressCommunication() {
    gather.progress();
    return !gather.isComplete();
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
    return sourcesDone && gather.isComplete();
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
      verifyResults(resultsVerifier, itr, null);
      return true;
    }
  }
}
