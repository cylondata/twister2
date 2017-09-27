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
package edu.iu.dsc.tws.rsched.spi.statemanager;

import com.google.common.util.concurrent.ListenableFuture;

import edu.iu.dsc.tws.common.config.Config;

import edu.iu.dsc.tws.proto.system.JobExecutionState;
import edu.iu.dsc.tws.proto.system.ResourceAPI;

/**
 * This file defines the IStateManager interface.
 * <p>
 * Services across Heron use HeronStateMgr to get/set state information.
 * Currently the primary things kept by state are:
 * 1. Where is the the topology master running.
 * The topology master is responsible for writing this information out
 * upon startup. The stream managers query this upon startup to find out
 * who is their topology master. In case they loose connection with
 * the topology master, the stream managers query this again to see
 * if the topology master has changed.
 * <p>
 * 2. Topology and the current running state of the topology
 * This information is seeded by the topology submitter.
 * The topology master updates this when the state of the topology
 * changes.
 * <p>
 * 3. Current assignment.
 * This information is solely used by topology master. When it
 * creates a new assignment or when the assignment changes, it writes
 * out this information. This is required for topology master failover.
 * <p>
 * Clients call the methods of the state passing a callback. The callback
 * is called with result code upon the completion of the operation.
 */
public interface IStateManager extends AutoCloseable {
  enum LockName {
    UPDATE_TOPOLOGY("updateJob");

    private String name;

    LockName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Initialize StateManager with the incoming context.
   */
  void initialize(Config config);

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the StateManager
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();

  /**
   * Return a lock object backed by the state manager store.
   *
   * @param topologyName the name of the topology
   * @param lockName any thread may get the {@code Lock} object bound to a given name, but only one
   * thread may obtain the actual lock from that @{code Lock} object.
   * @return an object representing an implementation of a lock.
   */
  Lock getLock(String topologyName, LockName lockName);

  /**
   * Is the given topology in RUNNING state?
   *
   * @return Boolean
   */
  ListenableFuture<Boolean> isJobRunning(String jobName);

  /**
   * Set the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setExecutionState(
      JobExecutionState.JobState executionState, String topologyName);

  /**
   * Set the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setSchedulerLocation(
      ResourceAPI.SchedulerLocation location, String topologyName);

  /**
   * Delete the tmaster location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteMasterLocation(String topologyName);

  /**
   * Delete the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteExecutionState(String topologyName);

  /**
   * Delete the topology definition for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteJob(String jobName);

  /**
   * Delete the physical plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deletePhysicalPlan(String topologyName);

  /**
   * Delete the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName);

  /**
   * Delete all locks for a given topology. Ideally locks should be deleted when released but it's
   * possible that some state systems (e.g., ZooKeeper) will not delete all resources when a lock is
   * released. This method should be invoked to clean all such lock resources.
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteLocks(String topologyName);
}
