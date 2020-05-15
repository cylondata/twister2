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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.nio.file.Paths;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;

public final class K8sControllerExample {
  private static final Logger LOG = Logger.getLogger(K8sControllerExample.class.getName());

  private K8sControllerExample() { }
  public static void main(String[] args) {
    String jobID = "ft-job";
    KubernetesController controller = new KubernetesController();
    controller.init("default");

    String configDir = "";
    String twister2Home = Paths.get(configDir).toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, "conf", "kubernetes");
    LOG.info("Loaded: " + config.size() + " configuration parameters.");

    testPVC(config, controller, jobID);
//    controller.deleteConfigMap(cmName);
//    testWorker(controller, jobID, 0);
//    testWorker(controller, jobID, 1);
//    testWorker(controller, jobID, 3);
//    testJM(controller, jobID);

    controller.close();
  }

  public static void testPVC(Config config, KubernetesController controller, String jobID) {

    RequestObjectBuilder.init(config, jobID, 0);
    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobID);
    V1PersistentVolumeClaim pvc =
        RequestObjectBuilder.createPersistentVolumeClaimObject(pvcName, 10);

    if (controller.existPersistentVolumeClaim(pvcName)) {
      controller.deletePersistentVolumeClaim(pvcName);
    } else {
      controller.createPersistentVolumeClaim(pvc);
    }

  }


  public static void createCM(KubernetesController controller, String jobID) {
    Config cnfg = Config.newBuilder()
        .put("nothing", "nothing")
        .build();

    RequestObjectBuilder.init(cnfg, jobID, 0);
    V1ConfigMap cm = RequestObjectBuilder.createConfigMap(10);

    controller.createConfigMap(cm);
  }

  public static void testWorker(KubernetesController controller, String jobID, int wID) {

    String keyName = KubernetesUtils.createRestartWorkerKey(wID);
    int restartCount = K8sWorkerUtils.initRestartFromCM(controller, jobID, keyName);
    LOG.info("restartCount for worker " + wID + ": " + restartCount);
  }

  public static void testJM(KubernetesController controller, String jobID) {

    String keyName = KubernetesUtils.createRestartJobMasterKey();
    int restartCount = K8sWorkerUtils.initRestartFromCM(controller, jobID, keyName);
    LOG.info("restartCount for jm: " + restartCount);
    JobMasterAPI.JobMasterState initialState = restartCount == 0
        ? JobMasterAPI.JobMasterState.JM_STARTED : JobMasterAPI.JobMasterState.JM_RESTARTED;

    LOG.info("jm initialState: " + initialState);
  }

  /**
   * test method
   */
  public static void test1() {

    String jobID = "ft-job";
    String jobID2 = "ft-job-2";
    Config cnfg = Config.newBuilder()
        .put("nothing", "nothing")
        .build();

    RequestObjectBuilder.init(cnfg, jobID, 0);
    V1ConfigMap cm = RequestObjectBuilder.createConfigMap(10);

    RequestObjectBuilder.init(cnfg, jobID2, 0);
    V1ConfigMap cm2 = RequestObjectBuilder.createConfigMap(10);

    KubernetesController controller = new KubernetesController();
    controller.init("default");

    String key0 = KubernetesUtils.createRestartWorkerKey(0);
    String key1 = KubernetesUtils.createRestartJobMasterKey();

    if (controller.existConfigMap(cm2.getMetadata().getName())) {
      controller.deleteConfigMap(cm2.getMetadata().getName());
    } else {
      controller.createConfigMap(cm2);

      int restartCount = controller.getRestartCount(jobID2, key0);
      LOG.info("restartCount: " + restartCount);
      controller.addRestartCount(jobID2, key0, 0);
      LOG.info("added restartCount: " + controller.getRestartCount(jobID2, key0));
      if (controller.updateRestartCount(jobID2, key0, 1)) {
        LOG.info("updated restartCount: " + controller.getRestartCount(jobID2, key0));
      } else {
        LOG.info("Cannot update restartCount");
        LOG.info("restartCount: " + controller.getRestartCount(jobID2, key0));
      }

      if (controller.updateRestartCount(jobID2, key1, 10)) {
        LOG.info("updated restartCount: " + controller.getRestartCount(jobID2, key1));
      } else {
        LOG.info("Cannot update restartCount");
        LOG.info("restartCount: " + controller.getRestartCount(jobID2, key1));
      }

      if (controller.removeRestartCount(jobID2, key0)) {
        LOG.info("removed restartCount: " + controller.getRestartCount(jobID2, key0));
      } else {
        LOG.info("cannot remove restartCount: " + controller.getRestartCount(jobID2, key0));
      }
    }

    if (controller.existConfigMap(cm.getMetadata().getName())) {
      controller.deleteConfigMap(cm.getMetadata().getName());
    } else {
      controller.createConfigMap(cm);
      int restartCount = controller.getRestartCount(jobID, key0);
      LOG.info("restartCount: " + restartCount);
    }

    LOG.info("done");
  }

}
