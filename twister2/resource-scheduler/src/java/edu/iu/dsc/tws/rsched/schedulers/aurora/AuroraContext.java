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
package edu.iu.dsc.tws.rsched.schedulers.aurora;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

/**
 * State parameters for Aurora Client
 */
public class AuroraContext extends SchedulerContext {

  public static final String AURORA_CLUSTER_NAME = "twister2.resource.scheduler.aurora.cluster";
  public static final String ROLE = "twister2.resource.scheduler.aurora.role";
  public static final String ENVIRONMENT = "twister2.resource.scheduler.aurora.env";

  public static final String AURORA_WORKER_CLASS = "twister2.class.aurora.worker";

  public static String auroraClusterName(Config cfg) {
    return cfg.getStringValue(AURORA_CLUSTER_NAME);
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(ROLE);
  }

  public static String environment(Config cfg) {
    return cfg.getStringValue(ENVIRONMENT);
  }

  public static String auroraWorkerClass(Config cfg) {
    return cfg.getStringValue(AURORA_WORKER_CLASS);
  }
}
