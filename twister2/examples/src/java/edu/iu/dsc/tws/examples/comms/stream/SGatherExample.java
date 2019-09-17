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
package edu.iu.dsc.tws.examples.comms.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.stream.SGather;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IntComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

public class SGatherExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SGatherExample.class.getName());

  private SGather gather;

  private boolean gatherDone = false;

  private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    if (jobParameters.getTargets() != 1) {
      LOG.warning("Setting targets to 1. Found, "
          + jobParameters.getTargets());
      jobParameters.getTaskStages().set(1, 1);
    }

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    );

    // create the communication
    gather = new SGather(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new FinalReduceReceiver());

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!logicalPlan.getLogicalIdsOfWorker(workerId).contains(
        logicalPlanBuilder.getTargets().iterator().next())) {
      gatherDone = true;
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (dataArray, args) -> {
      List<Tuple<Integer, int[]>> listOfArrays = new ArrayList<>();
      for (int i = 0; i < logicalPlanBuilder.getSources().size(); i++) {
        listOfArrays.add(new Tuple<>(i, dataArray));
      }
      return listOfArrays.iterator();
    }, new IteratorComparator<>(
        new TupleComparator<>(
            IntComparator.getInstance(),
            IntArrayComparator.getInstance()
        )
    ));

    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected boolean progressCommunication() {
    return gather.progress();
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
    return gatherDone && sourcesDone && gather.isComplete();
  }

  public class FinalReduceReceiver implements BulkReceiver {

    private int count = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {

    }

    @Override
    public boolean receive(int target, Iterator<Object> object) {
      count++;
      if (count > jobParameters.getWarmupIterations()) {
        Timing.mark(TIMING_MESSAGE_RECV, workerId == 0);
      }

      verifyResults(resultsVerifier, object, null);

      if (count == jobParameters.getTotalIterations()) {
        Timing.mark(TIMING_ALL_RECV, workerId == 0);
        BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, workerId == 0);
        resultsRecorder.writeToCSV();
        LOG.info(() -> String.format("Target %d received count %d", target, count));
        gatherDone = true;
      }
      return true;
    }
  }
}
