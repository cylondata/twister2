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
package edu.iu.dsc.tws.api.htgjob;

//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.logging.Logger;
//
//import com.google.protobuf.ByteString;
//
//import edu.iu.dsc.tws.api.JobConfig;
//import edu.iu.dsc.tws.common.config.Config;
//import edu.iu.dsc.tws.common.config.Context;
//import edu.iu.dsc.tws.common.worker.IWorker;
//import edu.iu.dsc.tws.comms.utils.KryoSerializer;
//import edu.iu.dsc.tws.proto.system.job.JobAPI;
//import edu.iu.dsc.tws.rsched.core.SchedulerContext;
//
///**
// * This is a basic job with only communication available
// */
//public final class Twister2HTGJob {
//
//  private static final Logger LOG = Logger.getLogger(Twister2HTGJob.class.getName());
//
//  private static final KryoSerializer KRYO_SERIALIZER = new KryoSerializer();
//
//  private String htgJobName;
//  private String workerClass;
//  private ArrayList<JobAPI.ComputeResource> computeResources = new ArrayList<>();
//  private JobConfig config;
//  private JobAPI.Job job;
//
//
//  private Twister2HTGJob() {
//  }
//
//  /**
//   * Serializing the JobAPI
//   **/
//  public JobAPI.HTGJob serialize() {
//
//    checkJobParameters();
//
//    JobAPI.HTGJob.Builder htgJobBuilder = JobAPI.HTGJob.newBuilder();
//    JobAPI.Config.Builder htgConfigBuilder = JobAPI.Config.newBuilder();
//
//    config.forEach((key, value) -> {
//      byte[] objectByte = KRYO_SERIALIZER.serialize(value);
//      htgConfigBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
//    });
//
//    htgJobBuilder.setConfig(htgConfigBuilder);
//    htgJobBuilder.setWorkerClassName(workerClass);
//    htgJobBuilder.setJob(job);
//    //htgJobBuilder.setJobName(htgJobName);
//    htgJobBuilder.setNumberOfWorkers(countNumberOfWorkers());
//
//    for (JobAPI.ComputeResource computeResource: computeResources) {
//      htgJobBuilder.addComputeResource(computeResource);
//    }
//
//    return htgJobBuilder.build();
//
//
//    /*JobAPI.Job.Builder jobBuilder = JobAPI.Job.newBuilder();
//    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();
//
//    config.forEach((key, value) -> {
//      byte[] objectByte = KRYO_SERIALIZER.serialize(value);
//      configBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
//    });
//
//    jobBuilder.setConfig(configBuilder);
//    jobBuilder.setWorkerClassName(workerClass);
//    jobBuilder.setJobName(jobName);
//    jobBuilder.setNumberOfWorkers(countNumberOfWorkers());
//
//    for (JobAPI.ComputeResource computeResource: computeResources) {
//      jobBuilder.addComputeResource(computeResource);
//    }
//
//    return jobBuilder.build();*/
//  }
//
//  private void checkJobParameters() {
//    if (htgJobName == null) {
//      throw new RuntimeException("Job jobName is null. You have to provide a unique jobName");
//    }
//
//    if (workerClass == null) {
//      throw new RuntimeException("workerClass is null. A worker class has to be provided.");
//    }
//
//    if (computeResources.size() == 0) {
//      throw new RuntimeException("No ComputeResource is provided.");
//    }
//
//    if (countNumberOfWorkers() == 0) {
//      throw new RuntimeException("0 worker instances requested.");
//    }
//  }
//
//  public String getHtgJobName() {
//    return htgJobName;
//  }
//
//  public void setHtgJobName(String htgJobName) {
//    this.htgJobName = htgJobName;
//  }
//
//  public String getWorkerClass() {
//    return workerClass;
//  }
//
//  public ArrayList<JobAPI.ComputeResource> getComputeResources() {
//    return computeResources;
//  }
//
//  public int getNumberOfWorkers() {
//    return countNumberOfWorkers();
//  }
//
//  public JobConfig getConfig() {
//    return config;
//  }
//
//  private int countNumberOfWorkers() {
//    int totalWorkers = 0;
//    for (JobAPI.ComputeResource computeResource: computeResources) {
//      totalWorkers += computeResource.getNumberOfWorkers();
//    }
//    return totalWorkers;
//  }
//
//  public static Twister2HTGJob loadTwister2Job(Config config, JobConfig jobConfig) {
//
//    // build and return the job
//    return Twister2HTGJob.newBuilder()
//        .setJobName(Context.jobName(config))
//        .setWorkerClass(SchedulerContext.workerClass(config))
//        .loadComputeResources(config)
//        .setConfig(jobConfig)
//        .build();
//
//  }
//
//  @Override
//  public String toString() {
//    String jobStr = "[jobName=" + htgJobName + "], [workerClass=" + workerClass + "]";
//    for (int i = 0; i < computeResources.size(); i++) {
//      JobAPI.ComputeResource cr = computeResources.get(i);
//      jobStr += String.format("\nComputeResource[%d]: cpu: %.1f, ram: %d MB, disk: %.1f GB, "
//          + "instances: %d, workersPerPod: %d", i, cr.getCpu(), cr.getRamMegaBytes(),
//          cr.getDiskGigaBytes(), cr.getNumberOfWorkers(), cr.getWorkersPerPod());
//    }
//    return jobStr;
//  }
//
//  public static Twister2HTGJobBuilder newBuilder() {
//    return new Twister2HTGJobBuilder();
//  }
//
//  public static final class Twister2HTGJobBuilder {
//    private Twister2HTGJob twister2Job;
//    private Twister2HTGJob job;
//    private int computeResourceIndex = 0;
//
//    private Twister2HTGJobBuilder() {
//      this.twister2Job = new Twister2HTGJob();
//    }
//
//    public Twister2HTGJobBuilder setJob(JobAPI.Job jb) {
//      twister2Job.job = jb;
//      return this;
//    }
//
//    public Twister2HTGJobBuilder setJobName(String name) {
//      twister2Job.htgJobName = name;
//      return this;
//    }
//
//    public Twister2HTGJobBuilder setWorkerClass(String workerClass) {
//      twister2Job.workerClass = workerClass;
//      return this;
//    }
//
//    public Twister2HTGJobBuilder setWorkerClass(Class<? extends IWorker> workerClass) {
//      twister2Job.workerClass = workerClass.getName();
//      return this;
//    }
//
//    public Twister2HTGJobBuilder addComputeResource(double cpu,
//                                                 int ramMegaBytes,
//                                                 int numberOfWorkers) {
//      addComputeResource(cpu, ramMegaBytes, 0, numberOfWorkers, 1);
//      return this;
//    }
//
//    public Twister2HTGJobBuilder addComputeResource(double cpu,
//                                                 int ramMegaBytes,
//                                                 double diskGigaBytes,
//                                                 int numberOfWorkers) {
//      addComputeResource(cpu, ramMegaBytes, diskGigaBytes, numberOfWorkers, 1);
//      return this;
//    }
//
//
//    public Twister2HTGJobBuilder addComputeResource(double cpu,
//                                                 int ramMegaBytes,
//                                                 double diskGigaBytes,
//                                                 int numberOfWorkers,
//                                                 int workersPerPod) {
//      JobAPI.ComputeResource computeResource = JobAPI.ComputeResource.newBuilder()
//          .setCpu(cpu)
//          .setRamMegaBytes(ramMegaBytes)
//          .setDiskGigaBytes(diskGigaBytes)
//          .setNumberOfWorkers(numberOfWorkers)
//          .setWorkersPerPod(workersPerPod)
//          .setIndex(computeResourceIndex++)
//          .build();
//
//      twister2Job.computeResources.add(computeResource);
//      return this;
//    }
//
//    @SuppressWarnings("unchecked")
//    private Twister2HTGJobBuilder loadComputeResources(Config config) {
//
//      List<Map<String, Number>> list =
//          (List) (config.get(SchedulerContext.WORKER_COMPUTE_RESOURCES));
//
//      for (Map<String, Number> computeResource: list) {
//        double cpu = (Double) computeResource.get("cpu");
//        int ram = (Integer) computeResource.get("ram");
//        double disk = (Double) computeResource.get("disk");
//        int instances = (Integer) computeResource.get("instances");
//        int workersPerPod = 1;
//        if (computeResource.get("workersPerPod") != null) {
//          workersPerPod = (Integer) computeResource.get("workersPerPod");
//        }
//        addComputeResource(cpu, ram, disk, instances, workersPerPod);
//      }
//      return this;
//    }
//
//    public Twister2HTGJobBuilder setConfig(JobConfig config) {
//      twister2Job.config = config;
//      return this;
//    }
//
//    public Twister2HTGJob build() {
//      if (twister2Job.config == null) {
//        twister2Job.config = new JobConfig();
//      }
//      return twister2Job;
//    }
//
//  }
//}
