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
import edu.iu.dsc.tws.python.util.PythonWorkerUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

import py4j.DefaultGatewayServerListener;
import py4j.GatewayServer;
import py4j.Py4JServerConnection;

public class PythonWorker implements BatchTSetIWorker {

  private static final Logger LOG = Logger.getLogger(PythonWorker.class.getName());

  private static void startPythonProcess(String pythonPath, int port,
                                         boolean bootstrap, String[] args) throws IOException {
    LOG.info("Starting python process : " + pythonPath);
    String[] newArgs = new String[args.length + 2];
    newArgs[0] = "python3";
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
    LOG.info("Python process ended.");
  }

  private static GatewayServer initJavaServer(int port, Object entryPoint,
                                              String pythonPath, boolean bootstrap, String[] args) {
    GatewayServer py4jServer = new GatewayServer(entryPoint, port);
    py4jServer.addListener(new DefaultGatewayServerListener() {

      @Override
      public void connectionStarted(Py4JServerConnection gatewayConnection) {
        LOG.info("Connection established");
      }

      @Override
      public void serverStarted() {
        LOG.info("Started java server on " + port);
        new Thread(() -> {
          try {
            startPythonProcess(pythonPath, port, bootstrap, args);
            py4jServer.shutdown();
          } catch (IOException e) {
            LOG.log(Level.SEVERE, "Error in starting python process");
          }
        }, "python-process" + (bootstrap ? "-bootstrap" : "")).start();
      }
    });
    return py4jServer;
  }

  public void execute(BatchTSetEnvironment env) {
    int port = 5000 + env.getWorkerID();
    Twister2Environment twister2Environment = new Twister2Environment(env);
    String tw2Home = env.getConfig().getStringValue("twister2.directory.home");
    String pythonFileName = env.getConfig().getStringValue("python_file");
    String[] args = (String[]) env.getConfig().get("args");
    String mainPy = new File(tw2Home, pythonFileName).getAbsolutePath();
    GatewayServer gatewayServer = initJavaServer(port, twister2Environment, mainPy, false, args);
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

    final BootstrapPoint bootstrapPoint = new BootstrapPoint();
    String mainPy = new File(pythonFile).getAbsolutePath();
    final GatewayServer gatewayServer = initJavaServer(4500, bootstrapPoint, mainPy, true, args);
    final Semaphore semaphore = new Semaphore(0);
    bootstrapPoint.setBootstrapListener(bootstrapPoint1 -> semaphore.release());
    LOG.info("Exchanging configurations...");
    gatewayServer.start();

    // this is to start Twister2 job in the main thread. Starting this inside callback, throws
    // twister2 errors inside python process, since callback is called by python
    semaphore.acquire();

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(bootstrapPoint.getConfigs());
    jobConfig.put("python_file", new File(pythonFile).getName());
    jobConfig.put("args", args);

    Twister2Job.Twister2JobBuilder twister2JobBuilder = Twister2Job.newBuilder()
        .setJobName(bootstrapPoint.getJobName())
        .setWorkerClass(PythonWorker.class)
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

    Config config = ResourceAllocator.loadConfig(Collections.emptyMap());
    Twister2Submitter.submitJob(twister2JobBuilder.build(), config);
  }
}
