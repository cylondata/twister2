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

import edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore;
import edu.iu.dsc.tws.common.config.Config;

public final class CheckpointingConfigurations {

  public static final String JOB_MASTER_USED = "twister2.job.master.used";
  public static final String CHECKPOINTING_ENABLED = "twister2.checkpointing.enable";
  public static final String CHECKPOINTING_STORE_CLASS = "twister2.checkpointing.store";
  public static final String CHECKPOINTING_SOURCE_FREQUNCY
      = "twister2.checkpointing.source.frequency";

  public static final String RESTORING_CHECKPOINTED_JOB = "RESTORING_CHECKPOINTED_JOB";

  private CheckpointingConfigurations() {
  }

  public static boolean isCheckpointingEnabled(Config config) {
    return config.getBooleanValue(CHECKPOINTING_ENABLED, false)
        && config.getBooleanValue(JOB_MASTER_USED, true);
  }

  public static String getCheckpointingStoreClass(Config config) {
    return config.getStringValue(CHECKPOINTING_STORE_CLASS,
        LocalFileStateStore.class.getCanonicalName());
  }

  public static boolean startingFromACheckpoint(Config config) {
    return config.getBooleanValue(RESTORING_CHECKPOINTED_JOB, false);
  }

  public static long getCheckPointingFrequency(Config config) {
    return config.getLongValue(CHECKPOINTING_SOURCE_FREQUNCY, 1000);
  }
}
