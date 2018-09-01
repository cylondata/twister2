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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosContext;

public final class BasicMesosJob {
  private static final Logger LOG = Logger.getLogger(BasicMesosJob.class.getName());
  private BasicMesosJob() {
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    System.out.println("read config values: " + config.size());
    System.out.println(config);

    int cpus = MesosContext.cpusPerContainer(config);
    int ramMegaBytes = MesosContext.ramPerContainer(config);
    double diskGigaBytes = Context.workerVolatileDisk(config);
    int containers = MesosContext.numberOfContainers(config);

    String jobName = SchedulerContext.jobName(config);
    jobName += "-" + System.currentTimeMillis();
    System.out.println("job name is " + jobName);
    WorkerComputeResource workerComputeResource =
        new WorkerComputeResource(cpus, ramMegaBytes, diskGigaBytes);
    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    String workerClass = SchedulerContext.workerClass(config);
    System.out.println("Worker class: " + workerClass);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName(jobName)
        .setWorkerClass(workerClass)
        .setRequestResource(workerComputeResource, containers)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);

    System.out.println("now terminating...");
    Twister2Submitter.terminateJob(twister2Job.getName(), config);
  }

}
