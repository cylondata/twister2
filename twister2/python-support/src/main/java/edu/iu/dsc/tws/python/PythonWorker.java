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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.local.LocalSubmitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

import py4j.DefaultGatewayServerListener;
import py4j.GatewayServer;
import py4j.Py4JServerConnection;

public class PythonWorker implements BatchTSetIWorker {

  private static final Logger LOG = Logger.getLogger(PythonWorker.class.getName());

  private static void startPythonProcess(String pythonPath, int port,
                                         boolean bootstrap) throws IOException {
    LOG.info("Starting python process : " + pythonPath);
    ProcessBuilder python3 = new ProcessBuilder().command("python3", pythonPath,
        Integer.toString(port));
    python3.environment().put("T2_PORT", Integer.toString(port));
    python3.environment().put("T2_BOOTSTRAP", Boolean.toString(bootstrap));

    python3.redirectErrorStream(true);
    Process exec = python3.start();

    BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
    String line = null;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
    LOG.info("Python process ended.");
  }

  private static GatewayServer initJavaServer(int port, Object entryPoint,
                                              String pythonPath, boolean bootstrap) {
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
            startPythonProcess(pythonPath, port, bootstrap);
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
    String mainPy = new File(".", "src/main/python/main.py").getAbsolutePath();
    GatewayServer gatewayServer = initJavaServer(port, twister2Environment, mainPy, false);
    gatewayServer.start();
  }

  public static void main(String[] args) {
    final BootstrapPoint bootstrapPoint = new BootstrapPoint();
    String mainPy = new File(".", "src/main/python/main.py").getAbsolutePath();
    final GatewayServer gatewayServer = initJavaServer(4500, bootstrapPoint, mainPy, true);
    bootstrapPoint.setBootstrapListener(bootstrapPoint1 -> {
      LocalSubmitter localSubmitter = LocalSubmitter.prepare(""
          + "/home/chathura/Code/twister2/twister2/config/src/yaml/conf/");

      JobConfig jobConfig = new JobConfig();

      Twister2Job.Twister2JobBuilder twister2JobBuilder = Twister2Job.newBuilder()
          .setJobName(bootstrapPoint1.getJobName())
          .setWorkerClass(PythonWorker.class)
          .setConfig(jobConfig);

      if (!bootstrapPoint1.getComputeResources().isEmpty()) {
        bootstrapPoint1.getComputeResources().forEach(computeResource -> {
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

      Config config = Config.newBuilder().putAll(bootstrapPoint1.getConfigs()).build();

      localSubmitter.submitJob(twister2JobBuilder.build(), config);
    });
    LOG.info("Exchanging configurations...");
    gatewayServer.start();
  }
}
