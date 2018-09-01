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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public final class BasicKubernetesJob {
  private static final Logger LOG = Logger.getLogger(BasicKubernetesJob.class.getName());

  private BasicKubernetesJob() {
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {

//    LoggingHelper.setupLogging(null, "logs", "client");
    LOG.info("Job submission time: " + System.currentTimeMillis());

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    LOG.fine("read config values: " + config.size() + "\n" + config);

    submitJob(config);
  }

  private static String convertToString(List<Map<String, List<String>>> outerList) {

    String allPairs = "";
    for (Map<String, List<String>> map: outerList) {
      for (String mapKey: map.keySet()) {
        List<String> innerList = map.get(mapKey);
        for (String listItem: innerList) {
          allPairs += listItem + ": " + mapKey + "\n";
        }
      }
    }

    return allPairs;
  }

  /**
   * submit the job
   */
  public static void submitJob(Config config) {

    double cpus = SchedulerContext.workerCPU(config);
    int ramMegaBytes = SchedulerContext.workerRAM(config);
    int workers = SchedulerContext.workerInstances(config);
    double diskGigaBytes = Context.workerVolatileDisk(config);
    String jobName = SchedulerContext.jobName(config);
    WorkerComputeResource workerComputeResource =
        new WorkerComputeResource(cpus, ramMegaBytes, diskGigaBytes);

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    String workerClass = SchedulerContext.workerClass(config);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName(jobName)
        .setWorkerClass(workerClass)
        .setRequestResource(workerComputeResource, workers)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
