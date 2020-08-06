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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Iterators;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BJoin;
import edu.iu.dsc.tws.comms.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.JoinedKeyedBenchWorker;

public class BJoinExample extends JoinedKeyedBenchWorker {

  private static final Logger LOG = Logger.getLogger(BKeyedPartitionExample.class.getName());

  private BJoin join;

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag, int tag) {
    while (!join.join(task, key, data, flag, tag)) {
      // lets wait a litte and try again
      join.progress();
    }
    return true;
  }

  @Override
  protected void compute(WorkerEnvironment workerEnv) {
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

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    join = new BJoin(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER,
        MessageTypes.INTEGER_ARRAY, MessageTypes.INTEGER_ARRAY,
        new JoinReceiver(), new SimpleKeyBasedSelector(), false,
        Comparator.comparingInt(o -> (Integer) o), CommunicationContext.JoinType.INNER,
        CommunicationContext.JoinAlgorithm.SORT);

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    // now initialize the workers

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new JoinedKeyedBenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    join.close();
  }

  @Override
  protected boolean progressCommunication() {
    join.progress();
    return !join.isComplete();
  }

  @Override
  protected boolean isDone() {
    return sourcesDone && join.isComplete();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    throw new UnsupportedOperationException("Join requires massage with tag value");
  }

  public class JoinReceiver implements BulkReceiver {
    private int count = 0;
    private int expected;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      expected = expectedIds.size() * jobParameters.getIterations();
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      LOG.log(Level.INFO, String.format("%d Received message %d count %d",
          workerId, target, Iterators.size(it)));
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    join.finish(src);
  }
}
