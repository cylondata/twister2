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
package edu.iu.dsc.tws.api.faulttolerance;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;

public class FaultToleranceContext extends Context {

  // A flag to enable/disable fault tolerance in Twister2
  // By default, it is disabled
  public static final String FAULT_TOLERANT = "twister2.fault.tolerant";
  public static final boolean FAULT_TOLERANT_DEFAULT = false;

  // FAILURE_TIMEOUT determines worker failures.
  // If workers do not send heartbeat messages for this many milli seconds,
  // they are assumed failed
  public static final String FAILURE_TIMEOUT = "twister2.fault.tolerance.failure.timeout";
  public static final int FAILURE_TIMEOUT_DEFAULT = 10 * 1000;

  public static final String FAILURE_RETRIES = "twister2.fault.tolerance.retries";

  public static boolean faultTolerant(Config cfg) {
    return cfg.getBooleanValue(FAULT_TOLERANT, FAULT_TOLERANT_DEFAULT);
  }

  public static int sessionTimeout(Config cfg) {
    return cfg.getIntegerValue(FAILURE_TIMEOUT, FAILURE_TIMEOUT_DEFAULT);
  }

  public static int failureRetries(Config cfg, int def) {
    return cfg.getIntegerValue(FAILURE_RETRIES, def);
  }
}
