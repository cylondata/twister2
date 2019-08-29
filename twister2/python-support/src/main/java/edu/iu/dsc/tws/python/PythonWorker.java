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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.worker.BatchTSetIWorker;
import edu.iu.dsc.tws.local.LocalSubmitter;
import py4j.DefaultGatewayServerListener;
import py4j.GatewayServer;
import py4j.Py4JServerConnection;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PythonWorker implements BatchTSetIWorker {

    private static final Logger LOG = Logger.getLogger(PythonWorker.class.getName());

    private void startPythonProcess(int port) throws IOException {
        String mainPy = new File(".", "src/main/python/main.py").getAbsolutePath();
        ProcessBuilder python3 = new ProcessBuilder().command("python3", mainPy, Integer.toString(port));
        python3.redirectErrorStream(true);

        Process exec = python3.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

    }

    public void execute(BatchTSetEnvironment env) {
        int port = 12345 + env.getWorkerID();
        Twister2Environment twister2Environment = new Twister2Environment(env);
        GatewayServer py4jServer = new GatewayServer(twister2Environment, port);
        py4jServer.addListener(new DefaultGatewayServerListener() {
            @Override
            public void connectionStopped(Py4JServerConnection gatewayConnection) {
                py4jServer.shutdown();
            }
        });
        LOG.info("Started java server on " + port);
        try {
            py4jServer.start();
            this.startPythonProcess(
                    port
            );
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Error in starting python process");
        }
    }

    public static void main(String[] args) {
        LocalSubmitter localSubmitter = LocalSubmitter.prepare("" +
                "/home/chathura/Code/twister2/twister2/config/src/yaml/conf/");

        JobConfig jobConfig = new JobConfig();
        Twister2Job twister2Job = Twister2Job.newBuilder()
                .setJobName("python-job")
                .setWorkerClass(PythonWorker.class)
                .addComputeResource(1, 512, 4)
                .setConfig(jobConfig)
                .build();

        localSubmitter.submitJob(twister2Job, Config.newBuilder().build());

//        for (int i = 0; i < 1; i++) {
//            new PythonWorker().execute(null, i,
//                    null, null, null);
//        }
    }
}
