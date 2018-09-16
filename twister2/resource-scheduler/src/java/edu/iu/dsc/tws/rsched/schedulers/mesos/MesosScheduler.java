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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import org.apache.curator.shaded.com.google.common.primitives.Longs;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import edu.iu.dsc.tws.common.config.Config;


public class MesosScheduler implements Scheduler {
  public static final Logger LOG = Logger.getLogger(MesosScheduler.class.getName());
  private final String jobName;
  private int taskIdCounter = 0;
  private Config config;
  private MesosController controller;
  private int completedTaskCounter = 0;
  private int totalTaskCount;
  private int workerCounter = 0;
  private int[] offerControl = new int[3];
  //private String jobMasterIP;
  private boolean mpiJob = true;

  public MesosScheduler(MesosController controller, Config mconfig, String jobName) {
    this.controller = controller;
    this.config = mconfig;
    totalTaskCount = MesosContext.numberOfContainers(config);
    this.jobName = jobName;
  }

  @Override
  public void registered(SchedulerDriver schedulerDriver,
                         Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
    LOG.info("Registered" + frameworkID);
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver,
                           Protos.MasterInfo masterInfo) {
    LOG.info("Re-registered");
  }

  public boolean contains(String[] nodes, Protos.Offer offer) {
    for (String node : nodes) {
      if (offer.getHostname().equals(node)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver,
                             List<Protos.Offer> offers) {

    int index = 0;
    String[] desiredNodes = MesosContext.getDesiredNodes(config).split(",");
    if (taskIdCounter < totalTaskCount) {
      for (Protos.Offer offer : offers) {

        if (!MesosContext.getDesiredNodes(config).equals("all")
            && !contains(desiredNodes, offer)) {
          continue;
        }
        LOG.info("Offer comes from host ...:" + offer.getHostname());
        if (controller.isResourceSatisfy(offer)) {

          //creates job directory on nfs
          MesosPersistentVolume pv = new MesosPersistentVolume(
              controller.createPersistentJobDirName(jobName), workerCounter);
          String persistentVolumeDir = pv.getJobDir().getAbsolutePath();

          Offer.Operation.Launch.Builder launch = Offer.Operation.Launch.newBuilder();
          for (int i = 0; i < MesosContext.containerPerWorker(config); i++) {

            //creates directory for each worker
            pv.getWorkerDir();

            Protos.TaskID taskId = buildNewTaskID();

            // int begin = MesosContext.getWorkerPort(config) + taskIdCounter * 100;
            // int end = begin + 30;

            Protos.TaskInfoOrBuilder taskBuilder = TaskInfo.newBuilder()
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addResources(buildResource("cpus", MesosContext.cpusPerContainer(config)))
                .addResources(buildResource("mem", MesosContext.ramPerContainer(config)))
                //.addResources(buildRangeResource("ports", begin, end))
                .setData(ByteString.copyFromUtf8("" + taskId.getValue()));

            //Docker specific configurations
            if (MesosContext.getUseDockerContainer(config).equals("true")) {

              Protos.Parameter jobNameParam = Protos.Parameter.newBuilder().setKey("env")
                  .setValue("JOB_NAME=" + jobName).build();

              Protos.Parameter workerIdParam = Protos.Parameter.newBuilder().setKey("env")
                  .setValue("WORKER_ID=" + workerCounter++).build();

              Protos.Parameter frameworkIdParam = Protos.Parameter.newBuilder().setKey("env")
                  .setValue("FRAMEWORK_ID=" + offer.getFrameworkId().getValue()).build();

              Protos.Parameter classNameParam = null;

              //worker 0 will be the job master.
              if (taskId.getValue().equals("0")) {

                ((TaskInfo.Builder) taskBuilder).setName("Job Master");

                classNameParam = Protos.Parameter.newBuilder().setKey("env")
                    .setValue("CLASS_NAME="
                        + "edu.iu.dsc.tws.rsched.schedulers.mesos.master.MesosJobMasterStarter")
                    .build();
              } else {
                if (mpiJob) {
                  if (taskId.getValue().equals("1")) {
                    ((TaskInfo.Builder) taskBuilder).setName("MPI Master " + taskId);
                    classNameParam = Protos.Parameter.newBuilder().setKey("env")
                        .setValue("CLASS_NAME="
                            + "edu.iu.dsc.tws.rsched.schedulers.mesos.mpi.MesosMPIMasterStarter")
                        .build();
                  } else {
                    ((TaskInfo.Builder) taskBuilder).setName("task " + taskId);
                    classNameParam = Protos.Parameter.newBuilder().setKey("env")
                        .setValue("CLASS_NAME="
                            + "edu.iu.dsc.tws.rsched.schedulers.mesos.mpi.MesosMPISlaveStarter")
                        .build();
                  }
                } else {
                  if (taskId.getValue().equals("1")) {
                    ((TaskInfo.Builder) taskBuilder).setName("MPI Master " + taskId);
                  } else {
                    ((TaskInfo.Builder) taskBuilder).setName("task " + taskId);
                  }
                  classNameParam = Protos.Parameter.newBuilder().setKey("env")
                      .setValue("CLASS_NAME="
                          + "edu.iu.dsc.tws.rsched.schedulers.mesos.MesosDockerWorker")
                      .build();
                }
              }

              // docker image info
              Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder
                  = Protos.ContainerInfo.DockerInfo.newBuilder();
              dockerInfoBuilder.setImage(MesosContext.getDockerImageName(config));
              Protos.NetworkInfo netInfo = Protos.NetworkInfo.newBuilder()
                  .setName(MesosContext.getMesosOverlayNetworkName(config)).build();
              dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.USER);
              dockerInfoBuilder.addParameters(jobNameParam);
              dockerInfoBuilder.addParameters(workerIdParam);
              dockerInfoBuilder.addParameters(classNameParam);
              dockerInfoBuilder.addParameters(frameworkIdParam);
              Protos.Volume volume = Protos.Volume.newBuilder()
                  .setContainerPath("/twister2/")
                  .setHostPath(".")
                  .setMode(Protos.Volume.Mode.RW)
                  .build();

              Protos.Volume persistentVolume = Protos.Volume.newBuilder()
                  .setContainerPath("/persistent-volume/")
                  .setHostPath(persistentVolumeDir)
                  .setMode(Protos.Volume.Mode.RW)
                  .build();

              //temporary solution for some jar packages
              Protos.Volume customJarsVolume = Protos.Volume.newBuilder()
                  .setContainerPath("/customJars/")
                  .setHostPath("/root/.twister2/repository/customJars")
                  .setMode(Protos.Volume.Mode.RW)
                  .build();

              // container info
              Protos.ContainerInfo.Builder containerInfoBuilder
                  = Protos.ContainerInfo.newBuilder();
              containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
              containerInfoBuilder.addVolumes(volume);
              containerInfoBuilder.addVolumes(persistentVolume);
              containerInfoBuilder.addVolumes(customJarsVolume);
              containerInfoBuilder.setDocker(dockerInfoBuilder.build());
              containerInfoBuilder.addNetworkInfos(netInfo);
              ((TaskInfo.Builder) taskBuilder).setContainer(containerInfoBuilder);
              ((TaskInfo.Builder) taskBuilder)
                  .setCommand(Protos.CommandInfo.newBuilder().setShell(false));
            } else {
              Protos.ExecutorInfo executorInfo =
                  controller.getExecutorInfo(jobName,
                      MesosPersistentVolume.WORKER_DIR_NAME_PREFIX + workerCounter);

              ((TaskInfo.Builder) taskBuilder)
                  .setExecutor(Protos.ExecutorInfo.newBuilder(executorInfo));

            }

            launch.addTaskInfos(((TaskInfo.Builder) taskBuilder).build());

          }

          List<Protos.OfferID> offerIds = new ArrayList<>();
          offerIds.add(offer.getId());
          List<Protos.Offer.Operation> operations = new ArrayList<>();
          Offer.Operation operation = Offer.Operation.newBuilder()
              .setType(Offer.Operation.Type.LAUNCH)
              .setLaunch(launch)
              .build();

          operations.add(operation);

          Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();
          schedulerDriver.acceptOffers(offerIds, operations, filters);
          offerControl[index]++;
          LOG.info("Offer from host " + offer.getHostname() + "has been accepted.");

        }

        if (taskIdCounter >= totalTaskCount - 1) {
          return;
        }
      }
    }
  }

  private Protos.TaskID buildNewTaskID() {
    return Protos.TaskID.newBuilder()
        .setValue(Integer.toString(taskIdCounter++)).build();
  }

  private Protos.Resource buildResource(String name, double value) {
    return Protos.Resource.newBuilder().setName(name)
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(buildScalar(value)).build();
  }

  private Protos.Resource buildRangeResource(String name, int begin, int end) {
    Protos.Value.Range range = Protos.Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
    Protos.Value.Ranges ranges = Protos.Value.Ranges.newBuilder().addRange(range).build();
    return Protos.Resource.newBuilder().setName(name)
        .setType(Protos.Value.Type.RANGES)
        .setRanges(ranges).build();
  }

  private Protos.Value.Scalar.Builder buildScalar(double value) {
    return Protos.Value.Scalar.newBuilder().setValue(value);
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver,
                             Protos.OfferID offerID) {
    LOG.warning("This offer's been rescinded. Tough luck, cowboy.");
  }

  @Override
  public void statusUpdate(SchedulerDriver schedulerDriver,
                           Protos.TaskStatus taskStatus) {

    LOG.info("Status update: " + taskStatus.getState() + " from "
        + taskStatus.getTaskId().getValue());
    if (taskStatus.getState() == Protos.TaskState.TASK_FINISHED) {
      completedTaskCounter++;
      LOG.info("Number of completed tasks: " + completedTaskCounter + "/" + totalTaskCount);
    } else if (taskStatus.getState() == Protos.TaskState.TASK_FAILED
        || taskStatus.getState() == Protos.TaskState.TASK_LOST
        || taskStatus.getState() == Protos.TaskState.TASK_KILLED) {
      LOG.severe("Aborting because task " + taskStatus.getTaskId().getValue()
          + " is in unexpected state "
          + taskStatus.getState().getValueDescriptor().getName()
          + " with reason '"
          + taskStatus.getReason().getValueDescriptor().getName() + "'"
          + " from source '"
          + taskStatus.getSource().getValueDescriptor().getName() + "'"
          + " with message '" + taskStatus.getMessage() + "'");
    }

    if (totalTaskCount == completedTaskCounter) {
      LOG.info("All tasks are finished. Stopping driver");
      schedulerDriver.stop();
    }

  }

  @Override
  public void frameworkMessage(SchedulerDriver schedulerDriver,
                               Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
    // System.out.println("Received message (scheduler): " + new String(bytes)
    //    + " from " + executorID.getValue());
    LOG.info("Executor id:" + executorID.getValue()
        + " Time: " + Longs.fromByteArray(bytes));
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    LOG.info("We got disconnected ");
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver,
                        Protos.SlaveID slaveID) {
    LOG.severe("Lost slave: " + slaveID);
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver,
                           Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
    LOG.severe("Lost executor on slave " + slaveID);
  }

  @Override
  public void error(SchedulerDriver schedulerDriver, String s) {
    LOG.severe("We've got errors : " + s);
  }
}
