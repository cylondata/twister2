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
package edu.iu.dsc.tws.python;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.python.util.PythonWorkerUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.rsched.schedulers.standalone.MPIContext;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;
import edu.iu.dsc.tws.tset.worker.CheckpointingBatchTSetIWorker;

import mpi.Info;
import mpi.MPI;
import mpi.MPIException;

import py4j.DefaultGatewayServerListener;
import py4j.GatewayServer;
import py4j.Py4JServerConnection;

public class PythonWorker implements BatchTSetIWorker {

  private static final Logger LOG = Logger.getLogger(PythonWorker.class.getName());

  private static final String PYTHON_PORT_OFFSET = "twister2.python.port";
  private static final String PYTHON_BIN = "twister2.python.bin";
  private static final String MPI_AWARE_PYTHON = "MPI_AWARE_PYTHON";

  private static void startPythonProcess(String pythonPath, int port,
                                         boolean bootstrap, String[] args,
                                         boolean useMPIIfAvailable,
                                         int worldSize,
                                         Config config) throws IOException, MPIException {
    LOG.info("Starting python process : " + pythonPath);

    String pythonBin = config.getStringValue(PYTHON_BIN, "python3");

    boolean useMPI = false;
    try {
      useMPI = useMPIIfAvailable && MPI.isInitialized();
    } catch (MPIException e) {
      LOG.warning("Error occurred when trying to check mpi status. "
          + "Python process will start without MPI support.");
    }

    if (useMPI) {
      LOG.info("Starting python process with MPI support...");

      StringBuilder envBuilder = new StringBuilder();
      envBuilder.append("T2_PORT=")
          .append(port)
          .append(System.lineSeparator())
          .append("T2_BOOTSTRAP=")
          .append(bootstrap);

      Info info = new Info();
      info.set("env", envBuilder.toString());
      info.set("map_by", config.getStringValue(MPIContext.MPI_MAP_BY));

      String[] newArgs = new String[args.length + 1];
      newArgs[0] = pythonPath;
      System.arraycopy(args, 0, newArgs, 1, args.length);

      int[] errorCodes = new int[worldSize];
      MPI.COMM_WORLD.spawn(pythonBin, newArgs, worldSize, info, 0,
          errorCodes);
    } else {
      String[] newArgs = new String[args.length + 2];
      newArgs[0] = pythonBin;
      newArgs[1] = pythonPath;
      System.arraycopy(args, 0, newArgs, 2, args.length);

      ProcessBuilder python3 = new ProcessBuilder().command(newArgs);
      python3.environment().put("T2_PORT", Integer.toString(port));
      python3.environment().put("T2_BOOTSTRAP", Boolean.toString(bootstrap));

      python3.redirectErrorStream(true);
      Process exec = python3.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
      String line = null;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
      }
    }
  }

  private static GatewayServer initJavaServer(int port, EntryPoint entryPoint,
                                              String pythonPath, boolean bootstrap,
                                              String[] args, boolean useMPIIfAvailable,
                                              int worldSize, Config config) {
    GatewayServer py4jServer = new GatewayServer(entryPoint, port);
    py4jServer.addListener(new DefaultGatewayServerListener() {

      @Override
      public void connectionStarted(Py4JServerConnection gatewayConnection) {
        LOG.info("Connection established");
      }

      @Override
      public void connectionStopped(Py4JServerConnection gatewayConnection) {
        // this will release all the threads who are waiting on waitForCompletion()
        entryPoint.close();
      }

      @Override
      public void serverStarted() {
        LOG.info("Started java server on " + port);
        new Thread(() -> {
          try {
            startPythonProcess(pythonPath, port, bootstrap, args,
                useMPIIfAvailable, worldSize, config);
            entryPoint.waitForCompletion();
            py4jServer.shutdown();
          } catch (IOException e) {
            LOG.log(Level.SEVERE, "Error in starting python process");
          } catch (MPIException e) {
            LOG.log(Level.SEVERE, "Python process failed to start with MPI Support", e);
          } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, "Failed while waiting for the completion of python process", e);
          }
        }, "python-process" + (bootstrap ? "-bootstrap" : "")).start();
      }
    });
    return py4jServer;
  }

  public void execute(BatchTSetEnvironment env) {
    int port = env.getConfig().getIntegerValue(PYTHON_PORT_OFFSET) + env.getWorkerID();
    Twister2Environment twister2Environment = new Twister2Environment(env);
    String tw2Home = env.getConfig().getStringValue("twister2.directory.home");
    String pythonFileName = env.getConfig().getStringValue("python_file");
    String[] args = (String[]) env.getConfig().get("args");
    String mainPy = new File(tw2Home, pythonFileName).getAbsolutePath();

    boolean mpiAwarePython = env.getConfig().getBooleanValue(MPI_AWARE_PYTHON, false);

    if (mpiAwarePython) {
      LOG.info("Python has requested MPI awareness");
    }

    GatewayServer gatewayServer = initJavaServer(port, twister2Environment, mainPy,
        false, args, mpiAwarePython, env.getNoOfWorkers(), env.getConfig());
    gatewayServer.start();
    final Semaphore wait = new Semaphore(0);
    gatewayServer.addListener(new DefaultGatewayServerListener() {
      @Override
      public void serverStopped() {
        wait.release();
      }
    });
    try {
      wait.acquire();
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Failed while waiting for the server to stop", e);
    }
  }

  /**
   * This class will be used when twister2 is started with checkpointing enabled
   */
  public static class CheckpointablePythonWorker implements CheckpointingBatchTSetIWorker {

    private PythonWorker pythonWorker = new PythonWorker();

    @Override
    public void execute(CheckpointingTSetEnv env) {
      pythonWorker.execute(env);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    String pythonFile = System.getProperty("python_file");
    String mainFile = System.getProperty("main_file");

    boolean zip = pythonFile.endsWith("zip");
    if (zip) {
      try {
        String unzipDir = PythonWorkerUtils.unzip(pythonFile);
        pythonFile = new File(unzipDir, mainFile).getAbsolutePath();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed when extracting zip file.", e);
        return;
      }
    }

    Config config = ResourceAllocator.loadConfig(Collections.emptyMap());

    final BootstrapPoint bootstrapPoint = new BootstrapPoint();
    String mainPy = new File(pythonFile).getAbsolutePath();
    final GatewayServer gatewayServer = initJavaServer(
        config.getIntegerValue(PYTHON_PORT_OFFSET) - 1, bootstrapPoint, mainPy,
        true, args, false, 1, config);
    LOG.info("Exchanging configurations...");
    gatewayServer.start();

    bootstrapPoint.waitForCompletion();

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(bootstrapPoint.getConfigs());
    jobConfig.put("python_file", new File(pythonFile).getName());
    jobConfig.put("args", args);


    Twister2Job.Twister2JobBuilder twister2JobBuilder = Twister2Job.newBuilder()
        .setJobName(bootstrapPoint.getJobName())
        .setWorkerClass(
            CheckpointingContext.isCheckpointingEnabled(config)
                ? CheckpointablePythonWorker.class : PythonWorker.class
        )
        .setConfig(jobConfig);

    if (!bootstrapPoint.getComputeResources().isEmpty()) {
      bootstrapPoint.getComputeResources().forEach(computeResource -> {
        twister2JobBuilder.addComputeResource(
            computeResource.getCpu(),
            computeResource.getRam(),
            computeResource.getInstances()
        );
      });
    } else {
      LOG.warning("Computer resources not specified. Using default resource configurations.");
      twister2JobBuilder.addComputeResource(1, 512, 1);
    }

    Twister2Submitter.submitJob(twister2JobBuilder.build(), config);
  }
}
