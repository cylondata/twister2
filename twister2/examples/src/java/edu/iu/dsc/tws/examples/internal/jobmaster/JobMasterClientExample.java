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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IReceiverFromDriver;
import edu.iu.dsc.tws.api.resource.ISenderToDriver;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.worker.JMWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;

public final class JobMasterClientExample {
  private static final Logger LOG = Logger.getLogger(JobMasterClientExample.class.getName());

  private JobMasterClientExample() {
  }

  /**
   * a test class to run JMWorkerAgent
   * First, a JobMaster instance should be started on a machine
   * This client should connect to that server
   * <p>
   * It reads config files from conf/kubernetes directory
   * It uses the first ComputeResource in that config file as the ComputeResource of this worker
   * Number of workers is the number of workers in the first ComputeResource
   * <p>
   * When all workers joined, they get the full worker list
   * Then, each worker sends a barrier message
   * Then, each worker sends a completed message and closes
   */
  public static void main(String[] args) {

    if (args.length != 3) {
      LOG.severe("Provide jmAddress workerID and jobID as parameters.");
      return;
    }

    String jmAddress = args[0];
    int workerID = Integer.parseInt(args[1]);
    String jobID = args[2];

    // we assume that the twister2Home is the current directory
//    String configDir = "../twister2/config/src/yaml/";
    String configDir = "";
    String twister2Home = Paths.get(configDir).toAbsolutePath().toString();
    Config config1 = ConfigLoader.loadConfig(twister2Home, "conf/kubernetes");
    Config config2 = ConfigLoader.loadConfig(twister2Home, "conf/common");
    Config config = updateConfig(config1, config2, jmAddress);
    LOG.info("Loaded: " + config.size() + " configuration parameters.");

    Twister2Job twister2Job = Twister2Job.loadTwister2Job(config, null);
    twister2Job.setJobID(jobID);
    JobAPI.Job job = twister2Job.serialize();

    LOG.info("workerID: " + workerID);
    LOG.info("jobID: " + jobID);

    simulateClient(config, job, workerID);
  }

  /**
   * a method to simulate JMWorkerAgent running in workers
   */
  public static void simulateClient(Config config, JobAPI.Job job, int workerID) {

    String workerIP = JMWorkerController.convertStringToIP("localhost").getHostAddress();
    int workerPort = 10000 + (int) (Math.random() * 10000);
    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo("node.ip", "rack01", null);
    JobAPI.ComputeResource computeResource = job.getComputeResource(0);
    Map<String, Integer> additionalPorts = generateAdditionalPorts(config, workerPort);

    JobMasterAPI.WorkerInfo workerInfo = WorkerInfoUtils.createWorkerInfo(
        workerID, workerIP, workerPort, nodeInfo, computeResource, additionalPorts);

    int restartCount = K8sWorkerUtils.initialStateAndUpdate(config, job.getJobId(), workerInfo);

    long start = System.currentTimeMillis();
    WorkerRuntime.init(config, job, workerInfo, restartCount);
    long delay = System.currentTimeMillis() - start;
    LOG.severe("worker-" + workerID + " startupDelay " + delay);

    IWorkerStatusUpdater statusUpdater = WorkerRuntime.getWorkerStatusUpdater();
    IWorkerController workerController = WorkerRuntime.getWorkerController();
    ISenderToDriver senderToDriver = WorkerRuntime.getSenderToDriver();

    WorkerRuntime.addReceiverFromDriver(new IReceiverFromDriver() {
      @Override
      public void driverMessageReceived(Any anyMessage) {
        LOG.info("Received message from IDriver: \n" + anyMessage);

        senderToDriver.sendToDriver(anyMessage);
      }
    });

//    WorkerRuntime.addAllJoinedListener(new IAllJoinedListener() {
//      @Override
//      public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
//        LOG.info("All workers joined, IDs: " + getIDs(workerList));
//      }
//    });

    // wait up to 2sec
//    sleeeep((long) (Math.random() * 2 * 1000));

//    List<JobMasterAPI.WorkerInfo> workerList = workerController.getJoinedWorkers();
//    LOG.info("Currently joined worker IDs: " + getIDs(workerList));

    try {
      List<JobMasterAPI.WorkerInfo> workerList = workerController.getAllWorkers();
      LOG.info("All workers joined... IDs: " + getIDs(workerList));
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // wait
    sleeeep(2 * 1000);

    try {
      workerController.waitOnBarrier();
      LOG.info("All workers reached the barrier. Proceeding.......");
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }

//    int id = job.getNumberOfWorkers() - 1;
//    JobMasterAPI.WorkerInfo info = workerController.getWorkerInfoForID(id);
//    LOG.info("WorkerInfo for " + id + ": \n" + info);

    // wait up to 3sec
    sleeeep((long) (Math.random() * 10 * 1000));

    // start the worker
    try {
      throwException(workerID);
    } catch (Throwable t) {
      // update worker status to FAILED
      statusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.FAILED);
      WorkerRuntime.close();
//      properShutDown = true;
//      System.exit(1);
      throw t;
    }

    statusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.COMPLETED);

    WorkerRuntime.close();

    System.out.println("Client has finished the computation. Client exiting.");
  }

  public static void throwException(int workerID) {
    if (workerID == 0) {
      throw new RuntimeException("test exception");
    }
  }

  public static List<Integer> getIDs(List<JobMasterAPI.WorkerInfo> workerList) {
    return workerList.stream()
        .map(wi -> wi.getWorkerID())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * construct a Config object
   */
  public static Config updateConfig(Config config1, Config config2, String jmAddress) {
    Config cnfg = Config.newBuilder()
        .putAll(config1)
        .putAll(config2)
        .put(JobMasterContext.JOB_MASTER_IP, jmAddress)
        .build();
    return cnfg;
  }

  /**
   * generate the additional requested ports for this worker
   */
  public static Map<String, Integer> generateAdditionalPorts(Config config, int workerPort) {

    // if no port is requested, return null
    List<String> portNames = SchedulerContext.additionalPorts(config);
    if (portNames == null) {
      return null;
    }

    HashMap<String, Integer> ports = new HashMap<>();
    int i = 1;
    for (String portName : portNames) {
      ports.put(portName, workerPort + i++);
    }

    return ports;
  }

  public static void sleeeep(long duration) {

    LOG.info("Sleeping " + duration + "ms............");

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
