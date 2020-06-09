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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.schedulers.k8s.master.ConfigMapWatcher;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;

public final class K8sControllerExample {
  private static final Logger LOG = Logger.getLogger(K8sControllerExample.class.getName());

  private K8sControllerExample() {
  }

  public static void main(String[] args) throws InterruptedException {
    KubernetesController controller = new KubernetesController();
    controller.init("default");

    String configDir = "";
    String twister2Home = Paths.get(configDir).toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, "conf", "kubernetes");
    LOG.info("Loaded: " + config.size() + " configuration parameters.");

    int numberOfWorkers = 4;
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-world-job")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(.2, 128, numberOfWorkers)
        .setConfig(new JobConfig())
        .build();
    twister2Job.setUserName("au");
    JobAPI.Job job = twister2Job.serialize();
    LOG.info("jobID: " + job.getJobId());

    V1ConfigMap cm = controller.getJobConfigMap(job.getJobId());
    if (cm == null) {
      LOG.info("there is no cm for this job on k8s");
    } else {
      LOG.info("cm: " + cm.getMetadata().getName());
    }

//    testPVC(config, controller, jobID);
    createCM(controller, job);
    getJobFromConfigMap(controller, job.getJobId());
//    testWorker(controller, jobID, 0);
//    testWorker(controller, jobID, 1);
//    testWorker(controller, jobID, 3);
//    testJM(controller, jobID);
//    controller.addParamToConfigMap(jobID, "KILL_JOB", "true");
//    Thread.sleep(5000);
//    createCMWatcher(controller, jobID);
//    Thread.sleep(5000);
    controller.deleteConfigMap(job.getJobId());

    controller.close();
  }

  public static void testPVC(Config config, KubernetesController controller, String jobID) {

    RequestObjectBuilder.init(config, jobID, 0, 0, null);
    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobID);
    V1PersistentVolumeClaim pvc =
        RequestObjectBuilder.createPersistentVolumeClaimObject(pvcName, 10);

    if (controller.existPersistentVolumeClaim(pvcName)) {
      controller.deletePersistentVolumeClaim(pvcName);
    } else {
      controller.createPersistentVolumeClaim(pvc);
    }
  }

  public static void createCM(KubernetesController controller, JobAPI.Job job) {
    Config cnfg = Config.newBuilder()
        .put("nothing", "nothing")
        .build();

    RequestObjectBuilder.init(cnfg, job.getJobId(), 0, 0, null);
    V1ConfigMap cm = RequestObjectBuilder.createConfigMap(job);

    controller.createConfigMap(cm);
  }

  public static void createCMWatcher(KubernetesController controller, String jobID) {
    ConfigMapWatcher cmWatcher = new ConfigMapWatcher("default", jobID, controller, null);
    cmWatcher.start();
  }

  public static void getJobFromConfigMap(KubernetesController controller, String jobID) {

//    JobAPI.Job jb = controller.getJobFromConfigMap(jobID);
//    LOG.info("job id: " + jb.getJobId());
//    LOG.info("number of workers: " + jb.getNumberOfWorkers());
//    LOG.info("job class: " + jb.getWorkerClassName());
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
  public static void test1(JobAPI.Job job) {

    String jobID = "ft-job";
    String jobID2 = "ft-job-2";
    Config cnfg = Config.newBuilder()
        .put("nothing", "nothing")
        .build();

    RequestObjectBuilder.init(cnfg, jobID, 0, 0, null);
    V1ConfigMap cm = RequestObjectBuilder.createConfigMap(job);

    RequestObjectBuilder.init(cnfg, jobID2, 0, 0, null);
    V1ConfigMap cm2 = RequestObjectBuilder.createConfigMap(job);

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
      controller.addConfigMapParam(jobID2, key0, 0 + "");
      LOG.info("added restartCount: " + controller.getRestartCount(jobID2, key0));
      if (controller.updateConfigMapParam(jobID2, key0, 1 + "")) {
        LOG.info("updated restartCount: " + controller.getRestartCount(jobID2, key0));
      } else {
        LOG.info("Cannot update restartCount");
        LOG.info("restartCount: " + controller.getRestartCount(jobID2, key0));
      }

      if (controller.updateConfigMapParam(jobID2, key1, 10 + "")) {
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
