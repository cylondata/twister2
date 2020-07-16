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
import edu.iu.dsc.tws.deeplearning.process.ProcessManager;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;

public class PytorchSampleJob implements IWorker {

  private static String scriptPath = "";

  private static String pythonPath = "";

  private static int masterProcessInstances = 2;

  private static int workerProcessInstances = 2;


  private static final Logger LOG = Logger.getLogger(PytorchSampleJob.class.getName());

  @Override
  public void execute(Config config, JobAPI.Job job, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    int workerID = workerController.getWorkerInfo().getWorkerID();
    String helloKeyValue = config.getStringValue("hello-key");
    scriptPath = config.getStringValue("scriptPath");
    pythonPath = config.getStringValue("pythonPath");
    workerProcessInstances = config.getIntegerValue("workerProcessInstances", 2);
    masterProcessInstances = config.getIntegerValue("masterProcessInstances", 2);

    LOG.info(scriptPath + "," + pythonPath + "," + workerProcessInstances + ","
        + masterProcessInstances);
    try {
      connect();
    } catch (MPIException e) {
      e.printStackTrace();
    }
  }

  public static void connect() throws MPIException {
    int workerId = MPI.COMM_WORLD.getRank();
    System.out.println("Worker Id : " + workerId);
    String[] spawnArgv = new String[1];
    spawnArgv[0] = scriptPath;
    int maxprocs = workerProcessInstances;
    int[] errcode = new int[maxprocs];
    ProcessManager processManager = new ProcessManager();
    Intercomm workerComm = processManager.spawn(scriptPath, spawnArgv, maxprocs,
        MPI.INFO_NULL, 0, errcode);
  }


  public static void main(String[] args) throws RuntimeException {
    scriptPath = "";
    workerProcessInstances = 2;
    masterProcessInstances = 2;
    pythonPath = "";

    if (args.length < 3) {
      throw new RuntimeException("Need the following parameters: "
          + "--scriptPath <path to bash script which runs the programme> "
          + "--masterInstances <processes for master programme> "
          + "--workerInstances <number of child processes to be spawned> "
          + "--pythonPath <path to python executable> ");
    } else {
      scriptPath = ParameterTool.fromArgs(args).get("scriptPath");
      workerProcessInstances = ParameterTool.fromArgs(args).getInt("masterInstances");
      workerProcessInstances = ParameterTool.fromArgs(args).getInt("workerInstances");
      pythonPath = ParameterTool.fromArgs(args).get("pythonPath");
    }

    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("hello-key", "Twister2-Hello");
    jobConfig.put("scriptPath", scriptPath);
    jobConfig.put("workerProcessInstances", workerProcessInstances);
    jobConfig.put("masterProcessInstances", masterProcessInstances);
    jobConfig.put("pythonPath", pythonPath);

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("twister2-pytorch-sample")
        .setWorkerClass(PytorchSampleJob.class)
        .addComputeResource(2, 1024, masterProcessInstances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);

  }
}
