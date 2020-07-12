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
package edu.iu.dsc.tws.examples.basic;

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements Twister2Worker {

  private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnvironment) {

    int workerID = workerEnvironment.getWorkerId();
    Config config = workerEnvironment.getConfig();

    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");

    // lets do a log to indicate we are running
    LOG.info(String.format("Hello World from Worker %d; there are %d total workers "
            + "and I got a message: %s", workerID,
        workerEnvironment.getNumberOfWorkers(), helloKeyValue));

    waitSeconds(30);
  }

  private void waitSeconds(long seconds) {

    try {
      LOG.info("Sleeping " + seconds + " seconds. Will complete after that.");
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // lets take number of workers as an command line argument
    int numberOfWorkers = 4;
    if (args.length == 1) {
      numberOfWorkers = Integer.valueOf(args[0]);
    }

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("hello-key", "Twister2-Hello");

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-world-job")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(.2, 128, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
