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
package edu.iu.dsc.tws.master;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class JobMasterContext extends Context {

  // if true, the job master runs in the submitting client
  public static final boolean JOB_MASTER_RUNS_IN_CLIENT_DEFAULT = true;
  public static final String JOB_MASTER_RUNS_IN_CLIENT = "twister2.job.master.runs.in.client";

  // worker to master ping interval in milliseconds
  public static final long PING_INTERVAL_DEFAULT = 1000;
  public static final String PING_INTERVAL = "twister2.worker.ping.interval";

  // client to master ping interval in milliseconds
  public static final int JOB_MASTER_PORT_DEFAULT = 11111;
  public static final String JOB_MASTER_PORT = "twister2.job.master.port";

  public static boolean jobMasterRunsInClient(Config cfg) {
    return cfg.getBooleanValue(JOB_MASTER_RUNS_IN_CLIENT, JOB_MASTER_RUNS_IN_CLIENT_DEFAULT);
  }

  public static long pingInterval(Config cfg) {
    return cfg.getLongValue(PING_INTERVAL, PING_INTERVAL_DEFAULT);
  }

  public static int jobMasterPort(Config cfg) {
    return cfg.getIntegerValue(JOB_MASTER_PORT, JOB_MASTER_PORT_DEFAULT);
  }

}
