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
package edu.iu.dsc.tws.rsched.statemanagers.fs;

import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.statemanager.IStateManager;
import edu.iu.dsc.tws.rsched.spi.statemanager.Lock;
import edu.iu.dsc.tws.rsched.spi.statemanager.WatchCallback;

public abstract class FileSystemStateManager implements IStateManager {
  private static final Logger LOG = Logger.getLogger(FileSystemStateManager.class.getName());

  // Store the root address of the hierarchical file system
  protected String rootAddress;

  protected enum StateLocation {
    MASTER_LOCATION("masters", "Master location"),
    JOB("jobs", "Jobs"),
    PHYSICAL_PLAN("pplans", "Physical plan"),
    EXECUTION_STATE("executionstate", "Execution state"),
    SCHEDULER_LOCATION("schedulers", "Scheduler location"),
    LOCKS("locks", "Distributed locks");

    private final String dir;
    private final String name;

    StateLocation(String dir, String name) {
      this.dir = dir;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String getDirectory(String root) {
      return concatPath(root, dir);
    }

    public String getNodePath(String root, String job) {
      return concatPath(getDirectory(root), job);
    }

    public String getNodePath(String root, String job, String extraToken) {
      return getNodePath(root, String.format("%s__%s", job, extraToken));
    }

    private static String concatPath(String basePath, String appendPath) {
      return String.format("%s/%s", basePath, appendPath);
    }
  }

  protected abstract ListenableFuture<Boolean> nodeExists(String path);

  protected abstract ListenableFuture<Boolean> deleteNode(String path,
                                                          boolean deleteChildrenIfNecessary);

  protected abstract <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                         String path,
                                                                         Message.Builder builder);
  protected abstract Lock getLock(String path);

  protected String getStateDirectory(StateLocation location) {
    return location.getDirectory(rootAddress);
  }

  protected String getStatePath(StateLocation location, String jobName) {
    return location.getNodePath(rootAddress, jobName);
  }

  @Override
  public void initialize(Config config) {
    this.rootAddress = SchedulerContext.stateManegerRootPath(config);
    LOG.log(Level.FINE, "File system state manager root address: {0}", rootAddress);
  }

  @Override
  public Lock getLock(String jobName, LockName lockName) {
    return getLock(
        StateLocation.LOCKS.getNodePath(this.rootAddress, jobName, lockName.getName()));
  }

  @Override
  public ListenableFuture<Boolean> deleteMasterLocation(String jobName) {
    return deleteNode(StateLocation.MASTER_LOCATION, jobName);
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String jobName) {
    return deleteNode(StateLocation.SCHEDULER_LOCATION, jobName);
  }

  @Override
  public ListenableFuture<Boolean> deleteJob(String jobName) {
    return deleteNode(StateLocation.JOB, jobName);
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String jobName) {
    return deleteNode(StateLocation.EXECUTION_STATE, jobName);
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String jobName) {
    return deleteNode(StateLocation.PHYSICAL_PLAN, jobName);
  }

  @Override
  public ListenableFuture<Boolean> isJobRunning(String jobName) {
    return nodeExists(getStatePath(StateLocation.JOB, jobName));
  }

  @Override
  public ListenableFuture<Boolean> deleteLocks(String jobName) {
    boolean result = true;
    for (LockName lockName : LockName.values()) {
      String path =
          StateLocation.LOCKS.getNodePath(this.rootAddress, jobName, lockName.getName());
      ListenableFuture<Boolean> thisResult = deleteNode(path, true);
      try {
        if (!thisResult.get()) {
          result = false;
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.log(Level.WARNING, "Error while waiting on result of delete lock at " + thisResult, e);
      }
    }
    final SettableFuture<Boolean> future = SettableFuture.create();
    future.set(result);
    return future;
  }

  private ListenableFuture<Boolean> deleteNode(StateLocation location, String jobName) {
    return deleteNode(getStatePath(location, jobName), false);
  }

  private <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                              StateLocation location,
                                                              String jobName,
                                                              Message.Builder builder) {
    return getNodeData(watcher, getStatePath(location, jobName), builder);
  }
}
