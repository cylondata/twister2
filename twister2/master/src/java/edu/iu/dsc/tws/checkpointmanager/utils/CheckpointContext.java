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
package edu.iu.dsc.tws.checkpointmanager.utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class CheckpointContext extends Context {

  private static final String STATEBACKEND_DIRECTORY = "twister2.statebackend.directory";
  private static final String CHECKPOINT_RECOVERY = "twister2.statebackend.recovery";
  private static final String CHECKPOINT_ENABLE = "twister2.checkpoint.enable";
  private static final String BARRIER_INTERVAL = "twister2.barrier.interval";
  private static final String STATEBACKEND_DIRECTORY_DEFAULT = System.getProperty("user.home")
      + "/statebackend/";

  public static String getStatebackendDirectoryDefault(Config cfg) {
    return cfg.getStringValue(STATEBACKEND_DIRECTORY, STATEBACKEND_DIRECTORY_DEFAULT);
  }

  public static boolean getCheckpointRecovery(Config cfg) {
    return cfg.getBooleanValue(CHECKPOINT_RECOVERY, false);
  }

  public static boolean enableCheckpoint(Config cfg) {
    return cfg.getBooleanValue(CHECKPOINT_ENABLE, false);
  }

  public static int getBarrierInterval(Config cfg){
    return cfg.getIntegerValue(BARRIER_INTERVAL,10000);
  }
}
