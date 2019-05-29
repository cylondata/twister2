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
package edu.iu.dsc.tws.ftolerance.api;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;

public class CheckpointingContext extends Context {
  public static final String CHECKPOINT_STORE_CLASS = "twister2.checkpointing.store.class";
//  public static final String RESTORE_SNAPSHOT = "twister2.checkpointing.restore.checkpoint";
//  public static final String LAST_SNAPSHOT_FILE = "last.checkpoint";

  /**
   * Returns the checkpoint ID/ version to restore. If returned value is 0, no restore is required!
   */
/*  public static String restoreCheckpoint(Config cfg) {
    return cfg.getStringValue(RESTORE_SNAPSHOT, "");
  }*/

  public static String checkpointStoreClass(Config cfg) {
    return cfg.getStringValue(CHECKPOINT_STORE_CLASS,
        "edu.iu.dsc.tws.ftolerance.api.LocalFileStateStore");
  }

/**
 * housekeeping dir for checkpoints
 */
  public static String checkpointDir(Config cfg) {
    return TokenSub.substitute(cfg, "${TWISTER2_HOME}/checkpoint",
        Context.substitutions);
  }
}
