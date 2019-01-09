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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BJoin;
import edu.iu.dsc.tws.comms.api.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.DataGenerator;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;

/**
 * This example join performs a join between two tables, 1 : | Student ID | Name | and 2 :
 * | Student ID | Course ID |. The result will produce a joined (inner join) result of
 * | Student ID | Name | Course ID |
 */
public class BJoinStudentExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(BJoinStudentExample.class.getName());

  private BJoin join;

  private boolean joinDone = false;

  private Lock lock = new ReentrantLock();

  @Override
  protected void execute() {
    //Setting up the task plan for the join operation
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
    int target = noOfSourceTasks;

    if (!taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
      joinDone = true;
    }

    // create the join communication
    join = new BJoin(communicator, taskPlan, sources, targets, MessageType.INTEGER,
        MessageType.OBJECT, new JoinReceiver(), new SimpleKeyBasedSelector(), false);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);

    // now initialize the workers
    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  /**
   * Messages that are sent to the join operation are sent to the communication layer
   * @param task task id
   * @param key the key of the message
   * @param data the data for this message
   * @param flag flag that holds indicators such as END
   * @param tag Specifies which partition of the join this message belongs to
   * @return true, this method will wait till the message is accepted by the communication layer
   */
  protected boolean sendMessages(int task, Object key, Object data, int flag, int tag) {
    while (!join.partition(task, key, data, flag, tag)) {
      // lets wait a litte and try again
      join.progress();
    }
    return true;
  }

  @Override
  public void close() {
    join.close();
  }

  @Override
  protected void progressCommunication() {
    join.progress();
  }

  @Override
  protected boolean isDone() {
    return joinDone && sourcesDone && !join.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    throw new UnsupportedOperationException("Join requires massage with tag value");
  }

  /**
   * The final result receiver class, it will get an iterator object which contains the
   * key and value pairs of each result from the join operation
   */
  public class JoinReceiver implements BulkReceiver {
    private int expected;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      expected = expectedIds.size() * jobParameters.getIterations();
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      while (it.hasNext()) {
        ImmutablePair item = (ImmutablePair) it.next();
        LOG.info("Key " + item.getKey() + " : Value " + item.getValue());
      }
      joinDone = true;

      return true;
    }
  }

  /**
   * This class is responsible of generating and sending the message to the join operation,
   * This is essentially the source task for this join example.
   */
  protected class MapWorker implements Runnable {
    private int task;

    public MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker: " + workerId + " task: " + task);

      //Data for the join
      //Student Id's
      int[] keysStudent = {1, 2, 3, 4, 5, 6, 7, 8};

      //Student (For course list) Id's
      int[] keysCourse = {1, 2, 3, 4, 5, 6, 7, 8, 1, 3, 5, 1};
      //Student Names, which map to the student id's
      String[] names = {"John", "Peter", "Tedd", "Jake", "Matt", "Adam", "Max", "Roger"};

      //Course Names which map to the keysCourse array
      String[] courses = {"E342", "E542", "E242", "E342", "E347", "E347", "E101", "E241", "E247",
          "E101", "E541", "E333"};
      int[] dataLeft = DataGenerator.generateIntData(jobParameters.getSize());

      // Each task will only send out data for students who have a student id of (task id + 1)
      // This is done for demonstration purposes to make sure no two tasks send the same data points
      // Which would result in duplicate entries in the join results
      int flag = MessageFlags.LAST;
      for (int i = 0; i < keysStudent.length; i++) {
        if (keysStudent[i] == task + 1) {
          sendMessages(task, new Integer(keysStudent[i]), names[i], flag, 0);
        }

      }
      for (int i = 0; i < keysCourse.length; i++) {
        if (keysCourse[i] == task + 1) {
          sendMessages(task, new Integer(keysCourse[i]), courses[i], flag, 1);
        }

      }
      LOG.info(String.format("%d Done sending", workerId));
      lock.lock();
      finishedSources.put(task, true);
      boolean allDone = true;
      for (Map.Entry<Integer, Boolean> e : finishedSources.entrySet()) {
        if (!e.getValue()) {
          allDone = false;
        }
      }
      finishCommunication(task);
      sourcesDone = allDone;
      lock.unlock();
    }
  }

  @Override
  protected void finishCommunication(int src) {
    join.finish(src, 0);
    join.finish(src, 1);
  }
}
