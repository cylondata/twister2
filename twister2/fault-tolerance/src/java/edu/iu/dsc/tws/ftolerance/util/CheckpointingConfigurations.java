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
package edu.iu.dsc.tws.ftolerance.util;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.ftolerance.stores.LocalFileStateStore;

public class CheckpointingConfigurations {

  public static final String CHECKPOINTING_ENABLED = "twister2.checkpointing.enable";
  public static final String CHECKPOINTING_STORE_CLASS = "twister2.checkpointing.store";


  public static boolean isCheckpointingEnabled(Config config) {
    return config.getBooleanValue(CHECKPOINTING_ENABLED, false);
  }

  public static String getCheckpointingStoreClass(Config config) {
    return config.getStringValue(CHECKPOINTING_STORE_CLASS,
        LocalFileStateStore.class.getCanonicalName());
  }
}
