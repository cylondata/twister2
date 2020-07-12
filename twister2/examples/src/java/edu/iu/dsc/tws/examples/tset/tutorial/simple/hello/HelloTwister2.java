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

package edu.iu.dsc.tws.examples.tset.tutorial.simple.hello;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;

public class HelloTwister2 implements Twister2Worker {

  private static final Logger LOG = Logger.getLogger(HelloTwister2.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    LOG.info(String.format("Hello from worker %d", env.getWorkerID()));
  }

  public static void main(String[] args) {

    JobConfig jobConfig = new JobConfig();

    Twister2Job job = Twister2Job.newBuilder()
        .setJobName("hello-twister2")
        .setConfig(jobConfig)
        .setWorkerClass(HelloTwister2.class)
        .addComputeResource(1, 512, 4)
        .build();

    Twister2Submitter.submitJob(job);
  }

}
