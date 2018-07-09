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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesField;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

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

  private MPIMasterStarter() { }

  public static void main(String[] args) {

    String workerInstancesStr = System.getenv(Context.TWISTER2_WORKER_INSTANCES);
//    String workerInstancesStr = System.getenv("TWISTER2_WORKER_INSTANCES");
    LOG.info("workerInstancesStr: " + workerInstancesStr);
    int numberOfWorkers = Integer.parseInt(workerInstancesStr);

    String workersPerPodStr = System.getenv(KubernetesContext.WORKERS_PER_POD);
//    String workersPerPodStr = System.getenv("WORKERS_PER_POD");
    LOG.info("workersPerPodStr: " + workersPerPodStr);
    int workersPerPod = Integer.parseInt(workersPerPodStr);

    String namespace = System.getenv(KubernetesContext.KUBERNETES_NAMESPACE);
//    String namespace = System.getenv("KUBERNETES_NAMESPACE");
    String podIP = System.getenv(KubernetesField.POD_IP + "");

    int numberOfPods = numberOfWorkers / workersPerPod;
    String podName = System.getenv("HOSTNAME");
    String jobName = podName.substring(0, podName.lastIndexOf("-"));

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

    String[] mpirunCommand = mpirunCommand("Ring", workersPerPod);

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

  public static String[] mpirunCommand(String className, int workersPerPod) {
    return new String[]
        {"mpirun",
            "--hostfile",
            HOSTFILE_NAME,
            "--allow-run-as-root",
            "-npernode",
            workersPerPod + "",
            "java",
            className};
  }

  /**
   * sending a command to shell
   */
  public static boolean runProcess(String[] command) {
    StringBuilder stderr = new StringBuilder();
    boolean isVerbose = true;
    int status = ProcessUtils.runSyncProcess(false, command, stderr, new File("."), isVerbose);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to execute mpirun. Command=%s, STDERR=%s", commandAsAString(command), stderr));
    } else {
      LOG.info("mpirun is executed with the command: " + commandAsAString(command));
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

}
