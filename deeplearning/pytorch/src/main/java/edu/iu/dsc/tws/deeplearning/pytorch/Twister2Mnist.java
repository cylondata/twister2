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
package edu.iu.dsc.tws.deeplearning.pytorch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.deeplearning.common.ParameterTool;
import edu.iu.dsc.tws.deeplearning.io.ReadCSV;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class Twister2Mnist implements IWorker {

  private static final String TRAIN_PATH = "/tmp/twister2deepnet/mnist/train/";
  private static final String TEST_PATH = "/tmp/twister2deepnet/mnist/test/";

  private static final String TRAIN_DATA_FILE = "train-images-idx3-ubyte.csv";
  private static final String TRAIN_TARGET_FILE = "train-labels-idx1-ubyte.csv";

  private static final String TEST_DATA_FILE = "t10k-images-idx3-ubyte.csv";
  private static final String TEST_TARGET_FILE = "t10k-labels-idx1-ubyte.csv";

  private static String scriptPath = "";
  private static int workers = 2;

  private static final Logger LOG = Logger.getLogger(Twister2Mnist.class.getName());

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    initialize(config, workerID);
    loadFileFromDisk(workerID, workers);
  }

  private void initialize(Config config, int workerID) {
    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");
    scriptPath = config.getStringValue("scriptPath");
    workers = config.getIntegerValue("workers", 2);
    System.out.println("Worker Id: " + workerID + "," + scriptPath + "," + workers);
  }

  private void loadFileFromDisk(int workerID, int parallelism) {
    ReadCSV readTrainingData = new ReadCSV(TRAIN_PATH + TRAIN_DATA_FILE, 60000, 784, 0, workers);
    ReadCSV readTrainingTarget = new ReadCSV(TRAIN_PATH + TRAIN_TARGET_FILE, 60000, 1, 0,
        workers);
    ReadCSV readTestingData = new ReadCSV(TEST_PATH + TEST_DATA_FILE, 10000, 784, 0, workers);
    ReadCSV readTestingTarget = new ReadCSV(TEST_PATH + TEST_TARGET_FILE, 10000, 1, 0,
        workers);

    readTrainingData.read();
    readTrainingTarget.read();
    readTestingData.read();
    readTestingTarget.read();
  }

  public static void main(String[] args) {
    // lets take number of workers as an command line argument
    String localScriptPath = "";
    int localWorkers = 2;
    System.out.println("---------------------------");
    System.out.println(Arrays.toString(args));
    if (args.length < 2) {
      throw new RuntimeException("Invalid number of arguments");
    } else {
      localScriptPath = ParameterTool.fromArgs(args).get("scriptPath");
      localWorkers = ParameterTool.fromArgs(args).getInt("workers");
    }
    //String path = ParameterTool.fromArgs(args).get("scriptPath");
    //System.out.println("Script Path : " + path);
    System.out.println("---------------------------");

    int numberOfWorkers = 2;

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("hello-key", "Twister2-Spawn-Hello");
    jobConfig.put("scriptPath", localScriptPath);
    jobConfig.put("workers", localWorkers);

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-world-job-spawn")
        .setWorkerClass(Twister2Mnist.class)
        .addComputeResource(2, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
