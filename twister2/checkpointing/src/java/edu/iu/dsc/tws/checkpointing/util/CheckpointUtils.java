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
package edu.iu.dsc.tws.checkpointing.util;

import java.io.IOException;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;

public final class CheckpointUtils {

  private static final String JOB_CONFIG_STATE_PREFIX = "JOB_CONFIG_";
  private static final String JOB_META_STATE_PREFIX = "JOB_META_";

  private CheckpointUtils() {
  }

  public static void commitState(StateStore stateStore,
                                 String family,
                                 int componentIndex,
                                 SnapshotImpl snapshot,
                                 CheckpointingClient checkpointingClient,
                                 MessageHandler messageHandler) throws IOException {
    saveState(stateStore, snapshot);
    checkpointingClient.sendVersionUpdate(
        family,
        componentIndex,
        snapshot.getVersion(),
        messageHandler
    );
  }

  public static void saveState(StateStore stateStore, SnapshotImpl snapshot) throws IOException {
    stateStore.put(Long.toString(snapshot.getVersion()), snapshot.pack());
  }

  public static void restoreSnapshot(StateStore stateStore,
                                     Long version,
                                     SnapshotImpl snapshot) throws IOException {
    if (version == 0) {
      return;
    }
    byte[] stateBytes = stateStore.get(version.toString());
    if (stateBytes == null) {
      throw new RuntimeException("Couldn't find version " + version + " in store");
    }
    snapshot.unpack(stateBytes);
  }

  public static StateStore getStateStore(Config config) {
    String checkpointingStoreClass = CheckpointingContext.getCheckpointingStoreClass(config);
    try {
      return (StateStore) CheckpointingContext.class.getClassLoader()
          .loadClass(checkpointingStoreClass).newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Couldn't find checkpointing store class : "
          + checkpointingStoreClass);
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException("Failed to creat an instance of store : "
          + checkpointingStoreClass);
    } catch (ClassCastException classCastEx) {
      throw new RuntimeException("Instance of " + checkpointingStoreClass
          + " can't be casted to " + StateStore.class.toString());
    }
  }

  private static String getJobConfigKey(String jobId) {
    return JOB_CONFIG_STATE_PREFIX + jobId;
  }

  private static String getJobMetaKey(String jobId) {
    return JOB_META_STATE_PREFIX + jobId;
  }

  public static void saveJobConfigAndMeta(String jobId, byte[] jobMeta,
                                          byte[] jobConfig,
                                          StateStore stateStore) throws IOException {
    stateStore.put(getJobConfigKey(jobId), jobConfig);
    stateStore.put(getJobMetaKey(jobId), jobMeta);
  }

  public static byte[] restoreJobConfig(String jobId, StateStore stateStore) throws IOException {
    return stateStore.get(getJobConfigKey(jobId));
  }

  public static byte[] restoreJobMeta(String jobId, StateStore stateStore) throws IOException {
    return stateStore.get(getJobMetaKey(jobId));
  }

  public static boolean containsJobInStore(String jobId, StateStore stateStore) throws IOException {
    return stateStore.contains(getJobConfigKey(jobId)) && stateStore.contains(getJobMetaKey(jobId));
  }
}
