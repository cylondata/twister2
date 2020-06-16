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

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;

/**
 * Watch whether job resources are killed by Job Master
 * If necessary, kill the job resources from Twister2 client
 */

public class ClientJobKillHandler {
  private static final Logger LOG = Logger.getLogger(ClientJobKillHandler.class.getName());

  private String jobID;
  private KubernetesController controller;

  private List<String> ssList;
  private List<String> serviceList;
  private V1ConfigMap cm;
  private V1PersistentVolumeClaim pvc;

  public ClientJobKillHandler(String jobID, KubernetesController controller) {
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

  public boolean deleteUndeletedResources() {

    boolean allDeleted = true;
    for (String ssName: ssList) {
      if (!controller.deleteStatefulSet(ssName)) {
        allDeleted = false;
      }
    }
    for (String serviceName: serviceList) {
      if (!controller.deleteService(serviceName)) {
        allDeleted = false;
      }
    }

    if (cm != null) {
      if (!controller.deleteConfigMap(cm.getMetadata().getName())) {
        allDeleted = true;
      }
    }

    if (pvc != null) {
      if (!controller.deletePersistentVolumeClaim(pvc.getMetadata().getName())) {
        allDeleted = true;
      }
    }

    return allDeleted;
  }

  public String getUndeletedResources() {

    StringBuffer sb = new StringBuffer();
    ssList.forEach(ss -> sb.append("StatefulSet: " + ss + System.lineSeparator()));
    serviceList.forEach(ss -> sb.append("Service: " + ss + System.lineSeparator()));

    if (cm != null) {
      sb.append("ConfigMap: " + cm.getMetadata().getName() + System.lineSeparator());
    }

    if (pvc != null) {
      sb.append("PersistentVolumeClaim: " + pvc.getMetadata().getName() + System.lineSeparator());
    }

    return sb.toString();
  }

}
