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

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.proto.system.JobExecutionState;
import edu.iu.dsc.tws.proto.system.ResourceAPI;
import edu.iu.dsc.tws.rsched.spi.statemanager.Lock;
import edu.iu.dsc.tws.rsched.spi.statemanager.WatchCallback;
import edu.iu.dsc.tws.rsched.utils.FileUtils;

public class SharedFileSystemStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(SharedFileSystemStateManager.class.getName());

  /**
   * Local filesystem implementation of a lock that mimics the file system behavior of the
   * distributed lock.
   */
  private final class FileSystemLock implements Lock {
    private String path;

    private FileSystemLock(String path) {
      this.path = path;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
      long giveUpAtMillis = System.currentTimeMillis() + unit.toMillis(timeout);
      byte[] fileContents = Thread.currentThread().getName().getBytes(Charset.defaultCharset());
      while (true) {
        try {
          if (setData(this.path, fileContents, false).get()) {
            return true;
          } else if (System.currentTimeMillis() >= giveUpAtMillis) {
            return false;
          } else {
            TimeUnit.SECONDS.sleep(2); // need to pole the filesystem for availability
          }
        } catch (ExecutionException e) {
          // this is thrown when the file exists, which means the lock can't be obtained
        }
      }
    }

    @Override
    public void unlock() {
      deleteNode(this.path, false);
    }
  }

  // Make utils class protected for easy unit testing
  protected ListenableFuture<Boolean> setData(String path, byte[] data, boolean overwrite) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(path, data, overwrite);
    future.set(ret);

    return future;
  }

  @Override
  protected ListenableFuture<Boolean> nodeExists(String path) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.isFileExists(path);
    future.set(ret);

    return future;
  }

  @Override
  protected ListenableFuture<Boolean> deleteNode(String path, boolean deleteChildrenIfNecessary) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = true;
    if (FileUtils.isFileExists(path)) {
      if (!deleteChildrenIfNecessary && FileUtils.hasChildren(path)) {
        LOG.severe("delete called on a path with children but deleteChildrenIfNecessary is false: "
            + path);
        ret = false;
      } else {
        ret = FileUtils.deleteFile(path);
      }
    }
    future.set(ret);

    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <M extends Message> ListenableFuture<M> getNodeData(
      WatchCallback watcher, String path, Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();
    byte[] data = new byte[]{};
    if (FileUtils.isFileExists(path)) {
      data = FileUtils.readFromFile(path);
    }
    if (data.length == 0) {
      future.set(null);
      return future;
    }

    try {
      builder.mergeFrom(data);
      future.set((M) builder.build());
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse " + Message.Builder.class, e));
    }

    return future;
  }

  @Override
  protected Lock getLock(String path) {
    return new FileSystemLock(path);
  }

  @Override
  public void initialize(Config config) {
    super.initialize(config);

    if (!initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. "
          + "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    for (StateLocation location : StateLocation.values()) {
      String dir = getStateDirectory(location);
      LOG.fine(String.format("%s directory: %s", location.getName(), dir));
      if (!FileUtils.isDirectoryExists(dir) && !FileUtils.createDirectory(dir)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {

  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      JobExecutionState.JobState executionState, String jobName) {
    return setData(
        StateLocation.EXECUTION_STATE, jobName, executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      ResourceAPI.SchedulerLocation location, String jobName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a scheduler dies and
    // comes up deterministically.
    return setData(StateLocation.SCHEDULER_LOCATION, jobName, location.toByteArray(), true);
  }

  private ListenableFuture<Boolean> setData(FileSystemStateManager.StateLocation location,
                                            String jobName,
                                            byte[] bytes,
                                            boolean overwrite) {
    return setData(getStatePath(location, jobName), bytes, overwrite);
  }

}
