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

  // if true, the job master runs in the submitting client
  public static final boolean JOB_MASTER_ASSIGNS_WORKER_IDS_DEFAULT = true;
  public static final String JOB_MASTER_ASSIGNS_WORKER_IDS =
      "twister2.job.master.assigns.worker.ids";

  // worker to master ping interval in milliseconds
  public static final long PING_INTERVAL_DEFAULT = 10000;
  public static final String PING_INTERVAL = "twister2.worker.ping.interval";

  // client to master ping interval in milliseconds
  public static final int JOB_MASTER_PORT_DEFAULT = 11111;
  public static final String JOB_MASTER_PORT = "twister2.job.master.port";

  // client to master ping interval in milliseconds
  public static final String JOB_MASTER_IP_DEFAULT = null;
  public static final String JOB_MASTER_IP = "twister2.job.master.ip";

  // worker to master response wait time in milliseconds
  public static final long WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION_DEFAULT = 1000;
  public static final String WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION
      = "twister2.worker.to.job.master.response.wait.duration";

  public static boolean jobMasterAssignsWorkerIDs(Config cfg) {
    return cfg.getBooleanValue(JOB_MASTER_ASSIGNS_WORKER_IDS,
        JOB_MASTER_ASSIGNS_WORKER_IDS_DEFAULT);
  }

  public static boolean jobMasterRunsInClient(Config cfg) {
    return cfg.getBooleanValue(JOB_MASTER_RUNS_IN_CLIENT, JOB_MASTER_RUNS_IN_CLIENT_DEFAULT);
  }

  public static long pingInterval(Config cfg) {
    return cfg.getLongValue(PING_INTERVAL, PING_INTERVAL_DEFAULT);
  }

  public static int jobMasterPort(Config cfg) {
    return cfg.getIntegerValue(JOB_MASTER_PORT, JOB_MASTER_PORT_DEFAULT);
  }

  public static String jobMasterIP(Config cfg) {
    return cfg.getStringValue(JOB_MASTER_IP, JOB_MASTER_IP_DEFAULT);
  }

  public static long responseWaitDuration(Config cfg) {
    return cfg.getLongValue(WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION,
        WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION_DEFAULT);
  }

}
