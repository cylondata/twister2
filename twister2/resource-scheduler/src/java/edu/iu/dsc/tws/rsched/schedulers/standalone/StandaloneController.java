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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hashicorp.nomad.apimodel.Job;
import com.hashicorp.nomad.apimodel.JobListStub;
import com.hashicorp.nomad.apimodel.NetworkResource;
import com.hashicorp.nomad.apimodel.Port;
import com.hashicorp.nomad.apimodel.Resources;
import com.hashicorp.nomad.apimodel.Task;
import com.hashicorp.nomad.apimodel.TaskGroup;
import com.hashicorp.nomad.apimodel.Template;
import com.hashicorp.nomad.javasdk.EvaluationResponse;
import com.hashicorp.nomad.javasdk.NomadApiClient;
import com.hashicorp.nomad.javasdk.NomadApiConfiguration;
import com.hashicorp.nomad.javasdk.NomadException;
import com.hashicorp.nomad.javasdk.ServerQueryResponse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.IController;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public class StandaloneController implements IController {
  private static final Logger LOG = Logger.getLogger(StandaloneController.class.getName());

  private Config config;

  private boolean isVerbose;

  public StandaloneController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
  }

  @Override
  public boolean start(RequestedResources requestedResources, JobAPI.Job job) {
    String uri = StandaloneContext.nomadSchedulerUri(config);
    NomadApiClient nomadApiClient = new NomadApiClient(
        new NomadApiConfiguration.Builder().setAddress(uri).build());

    Job nomadJob = getJob(job, requestedResources);
    try {
      EvaluationResponse response = nomadApiClient.getJobsApi().register(nomadJob);
      LOG.log(Level.INFO, "Submitted job to nomad: " + response);
    } catch (IOException | NomadException e) {
      LOG.log(Level.SEVERE, "Failed to submit the job: ", e);
    } finally {
      closeClient(nomadApiClient);
    }
    return false;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean kill(JobAPI.Job job) {
    String jobName = job.getJobName();

    String uri = StandaloneContext.nomadSchedulerUri(config);
    LOG.log(Level.INFO, "Killing Job " + jobName);
    NomadApiClient nomadApiClient = new NomadApiClient(
        new NomadApiConfiguration.Builder().setAddress(uri).build());
    try {
      Job nomadJob = getRunningJob(nomadApiClient, job.getJobName());
      if (nomadJob == null) {
        LOG.log(Level.INFO, "Cannot find the running job: " + job.getJobName());
        return false;
      }
      nomadApiClient.getJobsApi().deregister(nomadJob.getId());
    } catch (RuntimeException | IOException | NomadException e) {
      LOG.log(Level.SEVERE, "Failed to terminate job " + jobName
          + " with error: " + e.getMessage(), e);
      return false;
    } finally {
      closeClient(nomadApiClient);
    }
    return true;
  }

  private void closeClient(NomadApiClient nomadApiClient) {
    try {
      if (nomadApiClient != null) {
        nomadApiClient.close();
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, String.format("Error closing client: %s", e.getMessage()), e);
    }
  }

  private Job getJob(JobAPI.Job job, RequestedResources resources) {
    String jobName = job.getJobName();
    Job nomadJob = new Job();
    nomadJob.setId(jobName);
    nomadJob.setName(jobName);
    nomadJob.setType("batch");
    nomadJob.addTaskGroups(getTaskGroup(job, resources));
    nomadJob.setDatacenters(Arrays.asList(StandaloneContext.NOMAD_DEFAULT_DATACENTER));
    nomadJob.setMeta(getMetaData(job));
    return nomadJob;
  }

  private static List<JobListStub> getRunningJobList(NomadApiClient apiClient) {
    ServerQueryResponse<List<JobListStub>> response;
    try {
      response = apiClient.getJobsApi().list();
    } catch (IOException | NomadException e) {
      LOG.log(Level.SEVERE, "Error when attempting to fetch job list", e);
      throw new RuntimeException(e);
    }
    return response.getValue();
  }

  private static Job getRunningJob(NomadApiClient apiClient, String jobName) {
    List<JobListStub> jobs = getRunningJobList(apiClient);
    for (JobListStub job : jobs) {
      Job jobActual;
      try {
        jobActual = apiClient.getJobsApi().info(job.getId()).getValue();
      } catch (IOException | NomadException e) {
        String msg = "Failed to retrieve job info for job " + job.getId()
            + " part of job " + jobName;
        LOG.log(Level.SEVERE, msg, e);
        throw new RuntimeException(msg, e);
      }
      if (jobName.equals(jobActual.getName())) {
        return jobActual;
      }
    }
    return null;
  }

  private TaskGroup getTaskGroup(JobAPI.Job job, RequestedResources resources) {
    TaskGroup taskGroup = new TaskGroup();
    taskGroup.setCount(resources.getNumberOfWorkers());
    taskGroup.setName(job.getJobName());
    taskGroup.addTasks(getShellDriver(job, resources));
    return taskGroup;
  }

  private static Map<String, String> getMetaData(JobAPI.Job job) {
    String jobName = job.getJobName();
    Map<String, String> metaData = new HashMap<>();
    metaData.put(StandaloneContext.NOMAD_JOB_NAME, jobName);
    return metaData;
  }

  private Task getShellDriver(JobAPI.Job job, RequestedResources resources) {
    String taskName = job.getJobName();
    Task task = new Task();
    // get the job working directory
    String workingDirectory = StandaloneContext.workingDirectory(config);
    String jobWorkingDirectory = Paths.get(workingDirectory, job.getJobName()).toString();
    String configDirectoryName = Paths.get(workingDirectory,
        job.getJobName(), SchedulerContext.clusterType(config)).toString();

    String corePackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.corePackageFileName(config);
    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    String nomadScriptContent = getNomadScriptContent(config, configDirectoryName);

    task.setName(taskName);
    task.setDriver("raw_exec");
    task.addConfig(StandaloneContext.NOMAD_TASK_COMMAND, StandaloneContext.SHELL_CMD);
    String[] args = workerProcessCommand(workingDirectory, resources, job);
    task.addConfig(StandaloneContext.NOMAD_TASK_COMMAND_ARGS, args);
    Template template = new Template();
    template.setEmbeddedTmpl(nomadScriptContent);
    template.setDestPath(StandaloneContext.NOMAD_HERON_SCRIPT_NAME);
    task.addTemplates(template);

    Resources resourceReqs = new Resources();
    String portNamesConfig = StandaloneContext.networkPortNames(config);
    String[] portNames = portNamesConfig.split(",");
    // configure nomad to allocate dynamic ports
    Port[] ports = new Port[portNames.length];
    int i = 0;
    for (String p : portNames) {
      ports[i] = new Port().setLabel(p);
      i++;
    }
    NetworkResource networkResource = new NetworkResource();
    networkResource.addDynamicPorts(ports);
    resourceReqs.addNetworks(networkResource);

    Map<String, String> envVars = new HashMap<>();
    envVars.put(StandaloneContext.WORKING_DIRECTORY_ENV,
        StandaloneContext.workingDirectory(config));

    if (!StandaloneContext.sharedFileSystem(config)) {
      envVars.put(StandaloneContext.DOWNLOAD_PACKAGE_ENV, "false");
    } else {
      envVars.put(StandaloneContext.DOWNLOAD_PACKAGE_ENV, "true");
    }
    // we are putting the core packages as env variable
    envVars.put(StandaloneContext.CORE_PACKAGE_ENV, corePackageFile);
    envVars.put(StandaloneContext.JOB_PACKAGE_ENV, jobPackageFile);

    task.setEnv(envVars);
    task.setResources(resourceReqs);
    return task;
  }

  private String getScriptPath(Config cfg, String jWorkingDirectory) {
    String shellScriptName = StandaloneContext.shellScriptName(cfg);
    return Paths.get(jWorkingDirectory, shellScriptName).toString();
  }

  private String getNomadScriptContent(Config cfg, String jConfigDir) {
    String shellDirectoryPath = getScriptPath(cfg, jConfigDir);
    try {
      return new String(Files.readAllBytes(Paths.get(
          shellDirectoryPath)), StandardCharsets.UTF_8);
    } catch (IOException e) {
      String msg = "Failed to read nomad script from "
          + StandaloneContext.shellScriptName(cfg) + " . Please check file path! - "
          + shellDirectoryPath;
      LOG.log(Level.SEVERE, msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  private String[] workerProcessCommand(String workingDirectory,
                                        RequestedResources resourcePlan, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDirectory, job.getJobName()).toString();
    String configDirectoryName = Paths.get(workingDirectory,
        job.getJobName(), SchedulerContext.clusterType(config)).toString();

    // lets construct the mpi command to launch
    List<String> mpiCommand = workerProcessCommand(getScriptPath(config, configDirectoryName));
    Map<String, Object> map = workerCommandArguments(config, workingDirectory, resourcePlan, job);

    mpiCommand.add(map.get("procs").toString());
    mpiCommand.add(map.get("java_props").toString());
    mpiCommand.add(map.get("classpath").toString());
    mpiCommand.add(map.get("container_class").toString());
    mpiCommand.add(job.getJobName());
    mpiCommand.add(twister2Home);
    mpiCommand.add(twister2Home);
    LOG.log(Level.FINE, String.format("Command %s", mpiCommand));

    String[] array = new String[mpiCommand.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = mpiCommand.get(i);
    }
    return array;
  }

  private Map<String, Object> workerCommandArguments(Config cfg, String workingDirectory,
                                                     RequestedResources requestedResources,
                                                     JobAPI.Job job) {
    Map<String, Object> commands = new HashMap<>();
    // lets get the configurations
    commands.put("procs", requestedResources.getNumberOfWorkers());

    String jobClassPath = JobUtils.jobClassPath(cfg, job, workingDirectory);
    LOG.log(Level.INFO, "Job class path: " + jobClassPath);
    String systemClassPath = JobUtils.systemClassPath(cfg);
    String classPath = jobClassPath + ":" + systemClassPath;
    commands.put("classpath", classPath);
    commands.put("java_props", "");
    commands.put("container_class", job.getWorkerClassName());

    return commands;
  }

  private List<String> workerProcessCommand(String mpiScript) {
    List<String> slurmCmd;
    slurmCmd = new ArrayList<>(Collections.singletonList(mpiScript));
    return slurmCmd;
  }
}
