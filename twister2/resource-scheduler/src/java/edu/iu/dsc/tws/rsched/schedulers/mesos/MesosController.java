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

import java.util.logging.Logger;

import org.apache.mesos.Protos;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class MesosController {

  public static final Logger LOG = Logger.getLogger(MesosWorker.class.getName());
  private Config config;
  private String workerClass;

  public MesosController(Config mconfig) {
    this.config = mconfig;
    this.workerClass = MesosContext.mesosWorkerClass(config);
  }


  public Protos.FrameworkInfo getFrameworkInfo() {

    String frameworkName = MesosContext.MesosFrameworkName(config);
    Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo.newBuilder();
    builder.setFailoverTimeout(120000);
    builder.setUser("");
    builder.setName(frameworkName);
    return builder.build();
  }

  private Protos.CommandInfo.URI getJobUri() {
    Protos.CommandInfo.URI.Builder uriBuilder = Protos.CommandInfo.URI.newBuilder();
    uriBuilder.setValue(MesosContext.MesosFetchURI(config)
        + "/" + SchedulerContext.jobPackageFileName(config));
    uriBuilder.setExtract(true);
    return uriBuilder.build();
  }
  private Protos.CommandInfo.URI getCoreUri() {
    Protos.CommandInfo.URI.Builder uriBuilder = Protos.CommandInfo.URI.newBuilder();
    uriBuilder.setValue(MesosContext.MesosFetchURI(config)
        + "/" + SchedulerContext.corePackageFileName(config));
    uriBuilder.setExtract(true);
    return uriBuilder.build();
  }

  public Protos.CommandInfo getCommandInfo(String jobName, String workerName) {
    String command = "java -cp \"twister2-core/lib/*:twister2-job/libexamples-java.jar:"
        + "/root/.twister2/repository/twister2-core/lib/mesos-1.5.0.jar\" "
        + workerClass + " " + jobName + " " + workerName;
    Protos.CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();
    cmdInfoBuilder.addUris(getJobUri()); //mesos-fetcher uses this to fetch job
    cmdInfoBuilder.addUris(getCoreUri());
    cmdInfoBuilder.setValue(command);
    return cmdInfoBuilder.build();
  }

  public Protos.ExecutorInfo getExecutorInfo(String jobName, String executorName) {
    Protos.ExecutorInfo.Builder builder = Protos.ExecutorInfo.newBuilder();
    builder.setExecutorId(Protos.ExecutorID.newBuilder().setValue(executorName));
    builder.setCommand(getCommandInfo(jobName, executorName));
    builder.setName(executorName);
    return builder.build();
  }

  public boolean isResourceSatisfy(Protos.Offer offer) {
    double cpu = 0.0;
    double mem = 0.0;
    double disk = 0.0;
    for (Protos.Resource r : offer.getResourcesList()) {
      if (r.getName().equals("cpus")) {
        cpu = r.getScalar().getValue();
      }
      if (r.getName().equals("mem")) {
        mem = r.getScalar().getValue();
      }
      if (r.getName().equals("disk")) {
        disk = r.getScalar().getValue();
      }
    }
    if (cpu < MesosContext.cpusPerContainer(config) * MesosContext.containerPerWorker(config)) {
      LOG.info("CPU request can not be granted");
      return false;
    }
    if (mem < MesosContext.ramPerContainer(config) * MesosContext.containerPerWorker(config)) {
      LOG.info("Memory request can not be granted");
      return false;
    }
    if (disk < MesosContext.diskPerContainer(config) * MesosContext.containerPerWorker(config)) {
      LOG.info("disk request can not be granted");
      return false;
    }
    //offer satisfies the needed resources
    return true;
  }

  public String createPersistentJobDirName(String jobName) {
    return SchedulerContext.nfsServerPath(config) + "/" + jobName;
  }


}
