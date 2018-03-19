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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;

import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1beta2StatefulSet;

public class KubernetesLauncher implements ILauncher {

  private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

  private Config config;
  private KubernetesController controller;

  public KubernetesLauncher() {
    controller = new KubernetesController();
  }

  @Override
  public void initialize(Config conf) {
    this.config = conf;
    controller.init();
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @param resourceRequest requested resources
   * @return true if the request is granted
   */
  @Override
  public boolean launch(RequestedResources resourceRequest, JobAPI.Job job) {

    String jobName = job.getJobName();

    // first check whether there is a running service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    String serviceLabel = KubernetesUtils.createServiceLabel(jobName);
    String namespace = KubernetesContext.namespace(config);
    V1Service service = controller.getService(namespace, serviceName);

    // if there is no service, start one
    if (service == null) {
      int port = KubernetesContext.servicePort(config);
      int targetPort = KubernetesContext.serviceTargetPort(config);
      service = KubernetesUtils.createServiceObject(serviceName, serviceLabel, port, targetPort);
      boolean serviceCreated = controller.createService(namespace, service);
      if (!serviceCreated) {
        LOG.log(Level.SEVERE, "Service could not be created. Aborting submission");
        throw new RuntimeException();
      }

      // if there is already a service with the same name
    } else {
      LOG.log(Level.INFO, "There is already a service with the name: " + serviceName
          + "\nNo need to create a new service. Will use the existing one.");
    }

    // first check whether there is a StatefulSet with the same name,
    // if so, do not submit new job. Give a message and terminate
    // user needs to explicitly terminate that job
    V1beta2StatefulSet existingStatefulSet = controller.getStatefulSet(namespace, jobName);
    if (existingStatefulSet != null) {
      LOG.log(Level.SEVERE, "There is already a StatefulSet object in Kubernetes master "
          + "with the name: " + jobName + "\nFirst terminate this running job and resubmit. ");
      return false;
    }

    // create the StatefulSet for this job
    int containersPerPod = KubernetesContext.containersPerPod(config);
    V1beta2StatefulSet statefulSet =
        KubernetesUtils.createStatefulSetObjectForJob(jobName, resourceRequest, containersPerPod);

    boolean statefulSetCreated = controller.createStatefulSetJob(namespace, statefulSet);
    if (!statefulSetCreated) {
      return false;
    }

    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    int numberOfPods = statefulSet.getSpec().getReplicas();

    long start = System.currentTimeMillis();

//    boolean transferred =
//      controller.transferJobPackageSequentially(namespace, jobName, numberOfPods, jobPackageFile);

    boolean transferred =
        controller.transferJobPackageInParallel(namespace, jobName, numberOfPods, jobPackageFile);

    long duration = System.currentTimeMillis() - start;
    System.out.println("Transferring all files took: " + duration + " ms.");

    if (!transferred) {
      LOG.log(Level.SEVERE, "Transferring the job package to some pods failed. "
          + "Terminating the job");

      terminateJob(jobName);
      return false;
    }

    return true;
  }


  /**
   * Close up any resources
   */
  @Override
  public void close() {
  }

  /**
   * Terminate the Kubernetes Job
   */
  @Override
  public boolean terminateJob(String jobName) {

    String namespace = KubernetesContext.namespace(config);

    // first delete the StatefulSet
    boolean statefulSetDeleted = controller.deleteStatefulSetJob(namespace, jobName);

    // last delete the service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    boolean deleted = controller.deleteService(namespace, serviceName);

    return true;
  }
}
