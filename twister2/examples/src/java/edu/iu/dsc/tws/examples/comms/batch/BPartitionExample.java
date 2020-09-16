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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BPartition;
import edu.iu.dsc.tws.comms.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;

public class BPartitionExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BPartitionExample.class.getName());

  private BPartition partition;

  private ResultsVerifier<int[], Iterator<int[]>> resultsVerifier;

  @Override
  protected void compute(WorkerEnvironment workerEnv) {
    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    partition = new BPartition(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new PartitionReceiver(),
        new LoadBalanceSelector(), false);

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
      int lowestTarget = logicalPlanBuilder.getTargets()
          .stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
      int target = Integer.parseInt(args.get("target").toString());

      int toThisFromOneSource = jobParameters.getTotalIterations()
          / logicalPlanBuilder.getTargets().size();
      if (jobParameters.getTotalIterations()
          % logicalPlanBuilder.getTargets().size() > (target - lowestTarget)) {
        toThisFromOneSource++;
      }

      List<int[]> expectedData = new ArrayList<>();

      for (int i = 0; i < toThisFromOneSource * logicalPlanBuilder.getSources().size(); i++) {
        expectedData.add(ints);
      }

      return expectedData.iterator();
    }, new IteratorComparator<>(
        IntArrayComparator.getInstance()
    ));

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    partition.close();
  }

  @Override
  protected boolean progressCommunication() {
    partition.progress();
    return !partition.isComplete();
  }

  @Override
  protected boolean isDone() {
    return sourcesDone && partition.isComplete();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!partition.partition(task, data, flag)) {
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

  @Override
  protected void finishCommunication(int src) {
    partition.finish(src);
  }
}
