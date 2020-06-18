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

  // FAILURE_TIMEOUT determines worker failures.
  // If workers do not send heartbeat messages for this many milli seconds,
  // they are assumed failed
  public static final String FAILURE_TIMEOUT = "twister2.fault.tolerance.failure.timeout";
  public static final int FAILURE_TIMEOUT_DEFAULT = 10 * 1000;

  // maximum number of times to re-execute IWorker after some other workers failed in the job
  public static final String MAX_REEXECUTES = "twister2.fault.tolerance.max.reexecutes";
  public static final int MAX_REEXECUTES_DEFAULT = 5;

  // maximum number of times to re-start worker JVMs after failures
  public static final String MAX_WORKER_RESTARTS = "twister2.fault.tolerance.max.worker.restarts";
  public static final int MAX_WORKER_RESTARTS_DEFAULT = 5;

  // maximum number of times to re-start mpi jobs in vase of failures
  public static final String MAX_MPIJOB_RESTARTS = "twister2.fault.tolerance.max.mpijob.restarts";
  public static final int MAX_MPIJOB_RESTARTS_DEFAULT = 3;

  public static int sessionTimeout(Config cfg) {
    return cfg.getIntegerValue(FAILURE_TIMEOUT, FAILURE_TIMEOUT_DEFAULT);
  }

  public static int maxReExecutes(Config cfg) {
    return cfg.getIntegerValue(MAX_REEXECUTES, MAX_REEXECUTES_DEFAULT);
  }

  public static int maxWorkerRestarts(Config cfg) {
    return cfg.getIntegerValue(MAX_WORKER_RESTARTS, MAX_WORKER_RESTARTS_DEFAULT);
  }

  public static int maxMpiJobRestarts(Config cfg) {
    return cfg.getIntegerValue(MAX_MPIJOB_RESTARTS, MAX_MPIJOB_RESTARTS_DEFAULT);
  }

}
