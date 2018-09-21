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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class KMeansJobMain {

  private static final Logger LOG = Logger.getLogger(KMeansJobMain.class.getName());

  public static void main(String[] args) {
    LOG.log(Level.INFO, "KMeans job");
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("kmeans-job");
    jobBuilder.setWorkerClass(KMeansJob.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  private int size;

  private int iterations;

  private int col;

  private int containers;

  private String fileName;

  private int outstanding = 0;

  private boolean threads = false;

  private int printInterval = 0;

  private String dataType;

  private int dimension;

  private int k;

  private int numPoints;

  private String pointFile;

  private String centerFile;

  public KMeansJobMain(int size, int iterations, int col, int containers) {
    this.size = size;
    this.iterations = iterations;
    this.col = col;
    this.containers = containers;
  }

  /**
   * This method is to build the job parameters.
   */
  public static KMeansJobMain build(Config cfg) {
    int iterations = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_ITR));
    int size = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_SIZE));
    int col = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_COL));
    int containers = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_CONTAINERS));
    String fName = cfg.getStringValue(KMeansConstants.ARGS_FNAME);
    int outstanding = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_OUTSTANDING));
    Boolean threads = Boolean.parseBoolean(cfg.getStringValue(KMeansConstants.ARGS_THREADS));
    int pi = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_PRINT_INTERVAL));
    String type = cfg.getStringValue(KMeansConstants.ARGS_DATA_TYPE);

    String pointFile = cfg.getStringValue(KMeansConstants.ARGS_POINT);
    String centerFile = cfg.getStringValue(KMeansConstants.ARGS_CENTERS);
    int points = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_N_POINTS));
    int k = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_K));
    int d = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));

    LOG.info(String.format("Starting with arguments: "
            + "iter %d size %d col %d containers "
            + "%d file %s outstanding %d threads %b",
        iterations, size, col, containers, fName, outstanding, threads));

    KMeansJobMain jobParameters = new KMeansJobMain(size, iterations, col, containers);

    jobParameters.fileName = fName;
    jobParameters.outstanding = outstanding;
    jobParameters.threads = threads;
    jobParameters.printInterval = pi;
    jobParameters.dataType = type;
    jobParameters.k = k;
    jobParameters.pointFile = pointFile;
    jobParameters.centerFile = centerFile;
    jobParameters.numPoints = points;
    jobParameters.dimension = d;

    return jobParameters;
  }

  public int getSize() {
    return size;
  }

  public int getIterations() {
    return iterations;
  }

  public int getCol() {
    return col;
  }

  public int getContainers() {
    return containers;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public int getOutstanding() {
    return outstanding;
  }

  public void setOutstanding(int outstanding) {
    this.outstanding = outstanding;
  }

  public boolean isThreads() {
    return threads;
  }

  public void setThreads(boolean threads) {
    this.threads = threads;
  }

  public int getPrintInterval() {
    return printInterval;
  }

  public String getDataType() {
    return dataType;
  }

  public int getDimension() {
    return dimension;
  }

  public int getK() {
    return k;
  }

  public int getNumPoints() {
    return numPoints;
  }

  public String getPointFile() {
    return pointFile;
  }

  public String getCenterFile() {
    return centerFile;
  }

  @Override
  public String toString() {
    return "JobParameters{"
        + "size="
        + size
        + ", iterations="
        + iterations
        + ", col="
        + col
        + ", containers="
        + containers
        + '}';
  }
}
