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
  public static final boolean JOB_MASTER_RUNS_IN_CLIENT_DEFAULT = false;
  public static final String JOB_MASTER_RUNS_IN_CLIENT = "twister2.job.master.runs.in.client";

  // if true, the job master runs in the submitting client
  public static final boolean JOB_MASTER_ASSIGNS_WORKER_IDS_DEFAULT = true;
  public static final String JOB_MASTER_ASSIGNS_WORKER_IDS =
      "twister2.job.master.assigns.worker.ids";

  // worker to master ping interval in milliseconds
  public static final long PING_INTERVAL_DEFAULT = 10000;
  public static final String PING_INTERVAL = "twister2.worker.ping.interval";

  public static final int JOB_MASTER_PORT_DEFAULT = 11011;
  public static final String JOB_MASTER_PORT = "twister2.job.master.port";

  public static final String JOB_MASTER_IP = "twister2.job.master.ip";

  // worker to master response wait time in milliseconds
  public static final long WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION_DEFAULT = 1000;
  public static final String WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION
      = "twister2.worker.to.job.master.response.wait.duration";

  // job master cpu dedication as double
  public static final double JOB_MASTER_CPU_DEFAULT = 0.2;
  public static final String JOB_MASTER_CPU = "twister2.job.master.cpu";

  // job master ram size as MB
  public static final int JOB_MASTER_RAM_DEFAULT = 1024;
  public static final String JOB_MASTER_RAM = "twister2.job.master.ram";

  // job master volatile disk size in GB
  public static final double VOLATILE_VOLUME_DEFAULT = 1.0;
  public static final String VOLATILE_VOLUME = "twister2.job.master.volatile.volume.size";

  // job master volatile disk size in GB
  public static final double PERSISTENT_VOLUME_DEFAULT = 1.0;
  public static final String PERSISTENT_VOLUME = "twister2.job.master.persistent.volume.size";

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
    return cfg.getStringValue(JOB_MASTER_IP);
  }

  public static long responseWaitDuration(Config cfg) {
    return cfg.getLongValue(WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION,
        WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION_DEFAULT);
  }

  public static double volatileVolumeSize(Config cfg) {
    return cfg.getDoubleValue(VOLATILE_VOLUME, VOLATILE_VOLUME_DEFAULT);
  }

  public static boolean volatileVolumeRequested(Config cfg) {
    return volatileVolumeSize(cfg) > 0;
  }

  public static double persistentVolumeSize(Config cfg) {
    return cfg.getDoubleValue(PERSISTENT_VOLUME, PERSISTENT_VOLUME_DEFAULT);
  }

  public static boolean persistentVolumeRequested(Config cfg) {
    return persistentVolumeSize(cfg) > 0;
  }

  public static double jobMasterCpu(Config cfg) {
    return cfg.getDoubleValue(JOB_MASTER_CPU, JOB_MASTER_CPU_DEFAULT);
  }

  public static int jobMasterRAM(Config cfg) {
    return cfg.getIntegerValue(JOB_MASTER_RAM, JOB_MASTER_RAM_DEFAULT);
  }

}
