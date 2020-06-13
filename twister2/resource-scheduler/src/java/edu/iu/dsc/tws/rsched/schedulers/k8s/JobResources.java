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

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.config.Config;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;

/**
 * Kubernetes resources for a job
 * watch when they are cleared by Job Master
 */

public class JobResources {
  private static final Logger LOG = Logger.getLogger(JobResources.class.getName());

  private Config config;
  private String jobID;
  private KubernetesController controller;

  private List<String> ssList;
  private List<String> serviceList;
  private V1ConfigMap cm;
  private V1PersistentVolumeClaim pvc;

  public JobResources(Config config, String jobID, KubernetesController controller) {
    this.config = config;
    this.jobID = jobID;
    this.controller = controller;
  }

  public void getInitialState() {
    ssList = getSSList();
    serviceList = getServiceList();
    cm = controller.getJobConfigMap(jobID);
    pvc = controller.getJobPersistentVolumeClaim(jobID);
  }

  /**
   * check whether Job resources deleted
   * log deleted resources
   * return true if all deleted
   */
  public boolean allDeleted() {

    // check deleted StatefulSet objects
    if (!ssList.isEmpty()) {

      List<String> currentSSList = getSSList();
      Iterator<String> ssIterator = ssList.iterator();
      while (ssIterator.hasNext()) {
        String ssName = ssIterator.next();
        if (!currentSSList.contains(ssName)) {
          LOG.info("Deleted StatefulSet: " + ssName);
          ssIterator.remove();
        }
      }
    }

    // check deleted Service objects
    if (!serviceList.isEmpty()) {

      List<String> currentServiceList = getServiceList();
      Iterator<String> serviceIterator = serviceList.iterator();
      while (serviceIterator.hasNext()) {
        String serviceName = serviceIterator.next();
        if (!currentServiceList.contains(serviceName)) {
          LOG.info("Deleted Service: " + serviceName);
          serviceIterator.remove();
        }
      }
    }

    if (cm != null) {
      V1ConfigMap currentCM = controller.getJobConfigMap(jobID);
      if (currentCM == null) {
        LOG.info("Deleted ConfigMap: " + cm.getMetadata().getName());
        cm = null;
      }
    }

    if (pvc != null) {
      V1PersistentVolumeClaim currentPVC = controller.getJobPersistentVolumeClaim(jobID);
      if (currentPVC == null) {
        LOG.info("Deleted V1PersistentVolumeClaim: " + pvc.getMetadata().getName());
        pvc = null;
      }
    }

    return ssList.isEmpty() && serviceList.isEmpty() && cm == null && pvc == null;
  }

  /**
   * get list of StatefulSet names
   */
  private List<String> getSSList() {
    return controller.getJobStatefulSets(jobID)
        .stream()
        .map(ss -> ss.getMetadata().getName())
        .collect(Collectors.toList());
  }

  /**
   * get list of Service names
   */
  private List<String> getServiceList() {
    return controller.getJobServices(jobID)
        .stream()
        .map(ss -> ss.getMetadata().getName())
        .collect(Collectors.toList());
  }

  public void logUndeletedResources() {

    ssList.forEach(ss -> LOG.warning("StatefulSet not deleted in the time limit: " + ss));
    serviceList.forEach(ss -> LOG.warning("Service not deleted in the time limit: " + ss));

    if (cm != null) {
      LOG.warning("ConfigMap not deleted in the time limit: " + cm.getMetadata().getName());
    }

    if (pvc != null) {
      LOG.warning("PersistentVolumeClaim not deleted in the time limit: "
          + pvc.getMetadata().getName());
    }
  }

}
