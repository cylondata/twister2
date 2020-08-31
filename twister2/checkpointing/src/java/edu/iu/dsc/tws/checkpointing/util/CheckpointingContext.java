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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.FileSystemContext;
import edu.iu.dsc.tws.checkpointing.stores.HDFSFileStateStore;
import edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore;

public final class CheckpointingContext {

  public static final String JOB_MASTER_USED = "twister2.job.master.used";
  public static final String CHECKPOINTING_ENABLED = "twister2.checkpointing.enable";
  public static final String CHECKPOINTING_STORE_CLASS = "twister2.checkpointing.store";
  public static final String CHECKPOINTING_SOURCE_FREQUNCY
      = "twister2.checkpointing.source.frequency";

  // by default 600 seconds
  public static final int REQUEST_TIMEOUT_DEFAULT = 600;
  public static final String REQUEST_TIMEOUT = "twister2.checkpointing.request.timeout";

  public static final String CHECKPOINTING_RESTORE_JOB = "twister2.checkpointing.restore.job";

  private CheckpointingContext() {
  }

  public static boolean isCheckpointingEnabled(Config config) {
    return config.getBooleanValue(CHECKPOINTING_ENABLED, false)
        && config.getBooleanValue(JOB_MASTER_USED, true);
  }

  public static String getCheckpointingStoreClass(Config config) {
    String type = FileSystemContext.persistentStorageType(config);

    switch (type) {
      case "hdfs":
        return HDFSFileStateStore.class.getCanonicalName();

      case "nfs":
        return LocalFileStateStore.class.getCanonicalName();

      case "local":
        return LocalFileStateStore.class.getCanonicalName();

      default:
        return LocalFileStateStore.class.getCanonicalName();
    }
  }

  public static boolean startingFromACheckpoint(Config config) {
    return config.getBooleanValue(CHECKPOINTING_RESTORE_JOB, false);
  }

  public static long getCheckPointingFrequency(Config config) {
    return config.getLongValue(CHECKPOINTING_SOURCE_FREQUNCY, 1000);
  }

  public static long getRequestTimeout(Config config) {
    return 1000L * config.getIntegerValue(REQUEST_TIMEOUT, REQUEST_TIMEOUT_DEFAULT);
  }

  //todo: can checkpointing data be saved to nfs even above parameter is LocalFileStateStore
  public static boolean isNfsUsed(Config config) {
    return "edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore"
        .equals(getCheckpointingStoreClass(config));
  }
}
