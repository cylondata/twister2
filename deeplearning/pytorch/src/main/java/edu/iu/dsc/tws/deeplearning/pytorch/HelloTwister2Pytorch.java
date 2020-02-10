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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.deeplearning.common.ParameterTool;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;

/**
 * This is a Hello World with MPI for Deep learning
 * ./bin/twister2 submit standalone jar lib/libtwister2-deeplearning.jar
 * edu.iu.dsc.tws.deeplearning.pytorch.HelloTwister2Pytorch
 * --scriptPath /home/vibhatha/sandbox/pytorch/twsrunner --workers 2
 */
public class HelloTwister2Pytorch implements IWorker {

  private static String scriptPath = "";

  private static int workers = 2;


  private static final Logger LOG = Logger.getLogger(HelloTwister2Pytorch.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");
    scriptPath = config.getStringValue("scriptPath");
    workers = config.getIntegerValue("workers", 2);
    System.out.println("Worker Id: " + workerID + "," + scriptPath + "," + workers);

    try {
      int workerId = MPI.COMM_WORLD.getRank();
      System.out.println("Worker Id : " + workerId);
      String[] spawnArgv = new String[1];
      spawnArgv[0] = scriptPath;
      int maxprocs = workers;
      int[] errcode = new int[maxprocs];

      Intercomm child = MPI.COMM_WORLD.spawn("bash", spawnArgv, maxprocs,
          MPI.INFO_NULL, 0, errcode);
      int[] data = new int[]{1, 2, 3, 4};
      if (workerId == 0) {
        child.send(data, data.length, MPI.INT, 1, 0);
      }
      //executePythonScript(workerID);
    } catch (MPIException exception) {
      System.out.println("Exception " + exception.getMessage());
    }

  }

  public void executePythonScript(int workerId) throws IOException {
    String pythonScriptPath = scriptPath;
    String[] cmd = new String[2];
    cmd[0] = "/home/vibhatha/venv/ENV37/bin/python3"; // check version of installed python:
    // python -V
    cmd[1] = pythonScriptPath;

    // create runtime to execute external command
    Runtime rt = Runtime.getRuntime();
    Process pr = rt.exec(cmd);

    // retrieve output from python script
    BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
    String line = "";
    while ((line = bfr.readLine()) != null) {
      // display each output line form python script
      System.out.println("From Python => Tws WorkerId:" + workerId + "=" + line);
    }
  }


  public void info(int workerID, IWorkerController workerController, String helloKeyValue) {

    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    String workersStr = WorkerInfoUtils.workerListAsString(workerList);
    LOG.info("All workers have joined the job. Worker list: \n" + workersStr);
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
        .setWorkerClass(HelloTwister2Pytorch.class)
        .addComputeResource(2, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);


  }
}
