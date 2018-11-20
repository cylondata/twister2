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
package edu.iu.dsc.tws.examples.harp;

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.harp.HarpWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.harp.collective.BcastCollective;
import edu.iu.harp.combiner.ByteArrCombiner;
import edu.iu.harp.combiner.Operation;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.ByteArray;
import edu.iu.harp.worker.Workers;

public class HelloHarp extends HarpWorker {

  private static final Logger LOG = Logger.getLogger(HelloHarp.class.getName());

  @Override
  public void executeHarp(Config config, int workerID, int numberOfWorkers,
                          IWorkerController workerController, IPersistentVolume persistentVolume,
                          IVolatileVolume volatileVolume,
                          DataMap harpDataMap, Workers harpWorkers) {

    LOG.info(String.format("Hello from worker %s", workerID));

    byte[] helloArray = new byte["Hello from your master".getBytes().length];

    if (harpWorkers.isMaster()) {
      helloArray = "Hello from your master".getBytes();
    }
    ByteArray intArray = new ByteArray(helloArray, 0, helloArray.length);
    Partition<ByteArray> ap = new Partition<>(0, intArray);
    Table<ByteArray> helloTable = new Table<>(0, new ByteArrCombiner(Operation.SUM));
    helloTable.addPartition(ap);

    String contextName = "hello-harp-context";
    String operationName = "master-bcast";

    LOG.info(String.format("Calling broadcasting. Data before bcast : %s", new String(helloArray)));
    BcastCollective.broadcast(contextName,
        operationName, helloTable, harpWorkers.getMasterID(),
        true, harpDataMap, harpWorkers);
    harpDataMap.cleanOperationData(contextName,
        operationName);

    LOG.info(String.format("Broadcast done. Printing at worker %d : %s",
        workerID, new String(helloArray)));
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
    jobConfig.put("hello-harp-key", "Twister2-Hello-Harp");

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-harp-job")
        .setWorkerClass(HelloHarp.class)
        .addComputeResource(1, 512, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
