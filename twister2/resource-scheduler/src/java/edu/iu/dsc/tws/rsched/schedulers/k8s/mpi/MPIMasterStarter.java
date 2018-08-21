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
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;
import static edu.iu.dsc.tws.common.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

/**
 * This class is started in the first pod in a StatefulSet
 * This class will get the PodIP addresses from all pods in a job
 * When getting the IP addresses, it also waits for all pods to become running
 * It saves those IP addresses to hostfile
 * It checks whether password free ssh is enabled between this pod and
 * all other pods in the statefulset
 * It then executes mpirun command to start OpenMPI workers
 */
public final class MPIMasterStarter {
  private static final Logger LOG = Logger.getLogger(MPIMasterStarter.class.getName());

  private static final String HOSTFILE_NAME = "hostfile";
  private static Config config = null;
  private static String jobName = null;

  private MPIMasterStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    String jobMasterIP = System.getenv(K8sEnvVariables.JOB_MASTER_IP + "");
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "");
    jobName = System.getenv(K8sEnvVariables.JOB_NAME + "");
    if (jobName == null) {
      throw new RuntimeException("JobName is null");
    }

    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/"
        + KUBERNETES_CLUSTER_TYPE;

    config = K8sWorkerUtils.loadConfig(configDir);

    K8sWorkerUtils.initLogger(config, "mpiMaster");

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobName);
    jobDescFileName = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/" + jobDescFileName;
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

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

    LOG.info("MPIMaster information summary: \n"
        + "podName: " + podName + "\n"
        + "podIP: " + podIP + "\n"
        + "jobName: " + jobName + "\n"
        + "namespace: " + namespace + "\n"
        + "numberOfWorkers: " + numberOfWorkers + "\n"
        + "workersPerPod: " + workersPerPod + "\n"
        + "numberOfPods: " + numberOfPods
    );

    ArrayList<String> podNames = createPodNames(numberOfPods);

    long start = System.currentTimeMillis();
    int timeoutSeconds = 100;
    HashMap<String, String> podNamesIPs =
        PodWatchUtils.getRunningJobPodIPs(podNames, jobName, namespace, timeoutSeconds);

    if (podNamesIPs == null) {
      LOG.severe("Could not get all pods running. Aborting. "
          + "You need to terminate this job and resubmit it....");
      return;
    }

    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      String jobMasterPodName = KubernetesUtils.createJobMasterPodName(jobName);
      jobMasterIP = podNamesIPs.remove(jobMasterPodName);
    }
    LOG.info("Job Master IP address: " + jobMasterIP);

    long duration = System.currentTimeMillis() - start;
    LOG.info("Getting all pods running took: " + duration + " ms.");

    createHostFile(podIP, podNamesIPs);

    String classToRun = "edu.iu.dsc.tws.rsched.schedulers.k8s.mpi.MPIWorkerStarter";
    String[] mpirunCommand =
        generateMPIrunCommand(classToRun, workersPerPod, jobMasterIP, encodedNodeInfoList);

    // when all pods become running, sshd may have not started on some pods yet
    // it takes some time to start sshd, after pods become running
    // we check whether password free ssh is enabled from mpimaster pod to all other pods

    start = System.currentTimeMillis();
    String[] scriptCommand = generateCheckSshCommand(podNamesIPs);
    boolean pwdFreeSshOk = runScript(scriptCommand);
    duration = System.currentTimeMillis() - start;
    LOG.info("Checking password free access took: " + duration + " ms");

    if (pwdFreeSshOk) {
      executeMpirun(mpirunCommand);
    } else {
      LOG.severe("Password free ssh can not be setup among pods. Not running mpirun ...");
    }

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
   * create all pod names in this job, except the first worker pod (this pod)
   * @param numberOfPods
   * @return
   */
  public static ArrayList<String> createPodNames(int numberOfPods) {

    ArrayList<String> podNames = new ArrayList<>();
    for (int i = 1; i < numberOfPods; i++) {
      String podName = KubernetesUtils.podNameFromJobName(jobName, i);
      podNames.add(podName);
    }

    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      String jobMasterPodName = KubernetesUtils.createJobMasterPodName(jobName);
      podNames.add(jobMasterPodName);
    }

    return podNames;
  }

  public static String[] generateMPIrunCommand(String className,
                                               int workersPerPod,
                                               String jobMasterIP,
                                               String encodedNodeInfoList) {

    String jobMasterCLArgument = createJobMasterIPCommandLineArgument(jobMasterIP);

    return new String[]
        {"mpirun",
            "--hostfile",
            HOSTFILE_NAME,
            "--allow-run-as-root",
            "-npernode",
            workersPerPod + "",
            "-x",
            "KUBERNETES_SERVICE_HOST=" + System.getenv("KUBERNETES_SERVICE_HOST"),
            "-x",
            "KUBERNETES_SERVICE_PORT=" + System.getenv("KUBERNETES_SERVICE_PORT"),
            "java",
            className,
            jobMasterCLArgument,
            jobName,
            "'" + encodedNodeInfoList + "'"
        };
  }

  /**
   * send mpirun command to shell
   */
  public static boolean executeMpirun(String[] command) {
    StringBuilder stderr = new StringBuilder();
    boolean isVerbose = true;
    LOG.info("mpirun will be executed with the command: \n" + commandAsAString(command));

    int status = ProcessUtils.runSyncProcess(false, command, stderr, new File("."), isVerbose);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to execute mpirun command=%s, STDERR=%s", commandAsAString(command), stderr));
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

  /**
   * we send the jobMaster IP as a command line parameter to workers
   * we send it in the form of: "jobMasterIP=ip"
   */
  public static String createJobMasterIPCommandLineArgument(String value) {
    return "jobMasterIP=" + value;
  }

  /**
   * retrieve job master ip from the command line parameter
   */
  public static String getJobMasterIPCommandLineArgumentValue(String commandLineArgument) {
    if (commandLineArgument == null || !commandLineArgument.startsWith("jobMasterIP=")) {
      return null;
    }

    return commandLineArgument.substring(commandLineArgument.indexOf('=') + 1);
  }

  public static String[] generateCheckSshCommand(HashMap<String, String> podNamesIPs) {

    String[] command = new String[podNamesIPs.size() + 1];
    command[0] = "./check_pwd_free_ssh.sh";

    int index = 1;
    for (String ip: podNamesIPs.values()) {
      command[index] = ip;
      index++;
    }

    return command;
  }

  /**
   * send check ssh script run command to shell
   */
  public static boolean runScript(String[] command) {
    StringBuilder stderr = new StringBuilder();
    boolean isVerbose = true;
    String commandStr = commandAsAString(command);
    LOG.info("the script will be executed with the command: \n" + commandStr);

    int status = ProcessUtils.runSyncProcess(false, command, stderr, new File("."), isVerbose);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to execute the script file command=%s, STDERR=%s", commandStr, stderr));
    } else {
      LOG.info("script: check_pwd_free_ssh.sh execution completed with success...");
      if (stderr.length() != 0) {
        LOG.info("The error output:\n " + stderr.toString());
      }
    }
    return status == 0;
  }
}
