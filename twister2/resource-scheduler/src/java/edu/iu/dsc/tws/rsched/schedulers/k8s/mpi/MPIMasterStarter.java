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
package edu.iu.dsc.tws.rsched.schedulers.k8s.mpi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;
import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

/**
 * This class is started in the first pod in a StatefulSet
 * This class will get the PodIP addresses from all pods in a job
 * It saves those IP addresses to hostfile
 * It then executes mpirun command to start OpenMPI workers
 * It executes mpirun after all pods become Running.
 */
public final class MPIMasterStarter {
  private static final Logger LOG = Logger.getLogger(MPIMasterStarter.class.getName());

  private static final String HOSTFILE_NAME = "hostfile";
  private static Config config = null;

  private MPIMasterStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    String configDir = POD_MEMORY_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE
        + KUBERNETES_CLUSTER_TYPE;

    config = K8sWorkerUtils.loadConfig(configDir);

    K8sWorkerUtils.initLogger(config, "mpiMaster");

    int numberOfWorkers = Context.workerInstances(config);
    int workersPerPod = KubernetesContext.workersPerPod(config);
    String namespace = KubernetesContext.namespace(config);
    int numberOfPods = numberOfWorkers / workersPerPod;

    InetAddress localHost = null;
    try {
      localHost = InetAddress.getLocalHost();

    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Cannot get localHost.", e);
      throw new RuntimeException("Cannot get localHost.", e);
    }

    String podName = localHost.getHostName();
    String podIP = localHost.getHostAddress();
    String jobName = podName.substring(0, podName.lastIndexOf("-"));

    LOG.info("MPIMaster information summary: \n"
        + "podName: " + podName + "\n"
        + "podIP: " + podIP + "\n"
        + "jobName: " + jobName + "\n"
        + "namespace: " + namespace + "\n"
        + "numberOfWorkers: " + numberOfWorkers + "\n"
        + "workersPerPod: " + workersPerPod + "\n"
        + "numberOfPods: " + numberOfPods
    );

    ArrayList<String> podNames = createPodNames(jobName, numberOfPods);

    long start = System.currentTimeMillis();
    int timeoutSeconds = 100;
    HashMap<String, String> podNamesIPs =
        PodWatchUtils.getRunningPodIPs(podNames, jobName, namespace, timeoutSeconds);

    if (podNamesIPs == null) {
      LOG.severe("Could not get all pods running. Aborting. "
          + "You need to terminate this job and resubmit it....");
      return;
    }

    long duration = System.currentTimeMillis() - start;
    LOG.info("Getting all pods running took: " + duration + " ms.");

    createHostFile(podIP, podNamesIPs);

    // get job master IP address
    String jobMasterPodName = KubernetesUtils.createJobMasterPodName(jobName);
    start = System.currentTimeMillis();
    String jobMasterIP = PodWatchUtils.getJobMasterIP(jobMasterPodName, jobName, namespace, 100);
    duration = System.currentTimeMillis() - start;
    LOG.info("Job Master IP address: " + jobMasterIP);
    LOG.info("The time to get the Job Master IP address: " + duration + "ms");

    String classToRun = "edu.iu.dsc.tws.rsched.schedulers.k8s.mpi.MPIWorkerStarter";
    String[] mpirunCommand = mpirunCommand(classToRun, workersPerPod, jobMasterIP);

    // some times, sshd may have not been started on one of the pods
    // even though we received pod Running event
    // what should we do:
    // a) send some ssh request to each pod before running mpirun
    // b) rerun mpirun if it fails (if it fails for some other reasons, it will be rerun???)
    // c) wait some time before executing mpirun such as 2 seconds
    // d) register all pods to JobMaster and wait until getting pod ready for each pod
    //    this does not guarantee actually.
    //    Since even the pod itself does not know whether sshd started???

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.warning("Thread sleep interrupted.");
    }

    runProcess(mpirunCommand);

  }

  /**
   * create hostfile for mpirun command
   * first line in the file is the ip of this pod
   * other lines are unordered
   * each line has one ip
   * @return
   */
  public static boolean createHostFile(String podIP, HashMap<String, String> podNamesIPs) {

    try {
      StringBuffer bufferToLog = new StringBuffer();
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(HOSTFILE_NAME)));

      writer.write(podIP + System.lineSeparator());
      bufferToLog.append(podIP + System.lineSeparator());

      for (String ip: podNamesIPs.values()) {
        writer.write(ip + System.lineSeparator());
        bufferToLog.append(ip + System.lineSeparator());
      }

      writer.flush();
      //Close writer
      writer.close();

      LOG.info("File: " + HOSTFILE_NAME + " is written with the content:\n"
          + bufferToLog.toString());

      return true;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when writing the file: " + HOSTFILE_NAME, e);
      return false;
    }

  }

  /**
   * create all pod names in this StatefulSet except the first pod
   * @param jobName
   * @param numberOfPods
   * @return
   */
  public static ArrayList<String> createPodNames(String jobName, int numberOfPods) {

    ArrayList<String> podNames = new ArrayList<>();
    for (int i = 1; i < numberOfPods; i++) {
      String podName = KubernetesUtils.podNameFromJobName(jobName, i);
      podNames.add(podName);
    }

    return podNames;
  }

  public static String[] mpirunCommand(String className, int workersPerPod, String jobMasterIP) {

    String commandLineArgument = createJobMasterIPCommandLineArgument(jobMasterIP);

    return new String[]
        {"mpirun",
            "--hostfile",
            HOSTFILE_NAME,
            "--allow-run-as-root",
            "-npernode",
            workersPerPod + "",
            "java",
            className,
            commandLineArgument
        };
  }

  /**
   * sending a command to shell
   */
  public static boolean runProcess(String[] command) {
    StringBuilder stderr = new StringBuilder();
    boolean isVerbose = true;
    LOG.info("mpirun will be executed with the command: \n" + commandAsAString(command));

    int status = ProcessUtils.runSyncProcess(false, command, stderr, new File("."), isVerbose);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to execute mpirun. Command=%s, STDERR=%s", commandAsAString(command), stderr));
    } else {
      LOG.info("mpirun execution completed with success...");
      if (stderr.length() != 0) {
        LOG.info("The error output:\n " + stderr.toString());
      }
    }
    return status == 0;
  }

  public static String commandAsAString(String[] commandArray) {
    String command = "";
    for (String cmd: commandArray) {
      command += cmd + " ";
    }

    return command;
  }

  public static String createJobMasterIPCommandLineArgument(String value) {
    return "jobMasterIP=" + value;
  }

  public static String getJobMasterIPCommandLineArgumentValue(String commandLineArgument) {
    if (commandLineArgument == null || !commandLineArgument.startsWith("jobMasterIP=")) {
      return null;
    }

    return commandLineArgument.substring(commandLineArgument.indexOf('=') + 1);
  }

}
