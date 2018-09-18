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
package edu.iu.dsc.tws.api.task;

import edu.iu.dsc.tws.common.config.Config;

public final class TaskConfigurations {
  public static final String DEFAULT_PARALLELISM = "twister2.task.default.parallelism";

  public static final String DEFAULT_EDGE = "default";

  private TaskConfigurations() {
  }

  public static int getDefaultParallelism(Config cfg, int def) {
    return cfg.getIntegerValue(DEFAULT_PARALLELISM, def);
  }
}
