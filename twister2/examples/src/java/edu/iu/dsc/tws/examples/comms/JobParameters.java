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
package edu.iu.dsc.tws.examples.comms;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class JobParameters {
  private static final Logger LOG = Logger.getLogger(JobParameters.class.getName());

  private int size;

  private int iterations;

  private String operation;

  private int containers;

  private List<Integer> taskStages;

  private int gap;

  private String fileName;

  private int outstanding = 0;

  private boolean threads = false;

  private int printInterval = 0;

  private String dataType;

  private int initIterations = 0;

  private boolean doVerify = false;

  private boolean stream = false;

  public JobParameters(int size, int iterations, String col,
                       int containers, List<Integer> taskStages, int gap) {
    this.size = size;
    this.iterations = iterations;
    this.operation = col;
    this.containers = containers;
    this.taskStages = taskStages;
    this.gap = gap;
  }

  public int getSize() {
    return size;
  }

  public int getIterations() {
    return iterations;
  }

  public String getOperation() {
    return operation;
  }

  public int getContainers() {
    return containers;
  }

  public List<Integer> getTaskStages() {
    return taskStages;
  }

  public int getGap() {
    return gap;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileName() {
    return fileName;
  }

  public int getOutstanding() {
    return outstanding;
  }

  public void setOutstanding(int outstanding) {
    this.outstanding = outstanding;
  }

  public void setThreads(boolean threads) {
    this.threads = threads;
  }

  public boolean isThreads() {
    return threads;
  }

  public int getPrintInterval() {
    return printInterval;
  }

  public String getDataType() {
    return dataType;
  }

  public int getInitIterations() {
    return initIterations;
  }

  public boolean isDoVerify() {
    return doVerify;
  }

  public boolean isStream() {
    return stream;
  }

  public void setStream(boolean stream) {
    this.stream = stream;
  }

  public static JobParameters build(Config cfg) {
    int iterations = Integer.parseInt(cfg.getStringValue(Constants.ARGS_ITR));
    int size = Integer.parseInt(cfg.getStringValue(Constants.ARGS_SIZE));
    String col = cfg.getStringValue(Constants.ARGS_OPERATION);
    int containers = Integer.parseInt(cfg.getStringValue(Constants.ARGS_WORKERS));
    String taskStages = cfg.getStringValue(Constants.ARGS_TASK_STAGES);
    int gap = Integer.parseInt(cfg.getStringValue(Constants.ARGS_GAP));
    String fName = cfg.getStringValue(Constants.ARGS_FNAME);
    int outstanding = Integer.parseInt(cfg.getStringValue(Constants.ARGS_OUTSTANDING));
    Boolean threads = Boolean.parseBoolean(cfg.getStringValue(Constants.ARGS_THREADS));
    int pi = Integer.parseInt(cfg.getStringValue(Constants.ARGS_PRINT_INTERVAL));
    String type = cfg.getStringValue(Constants.ARGS_DATA_TYPE);
    int intItr = Integer.parseInt(cfg.getStringValue(Constants.ARGS_INIT_ITERATIONS));
    boolean doVerify = cfg.getBooleanValue(Constants.ARGS_VERIFY);
    boolean stream = cfg.getBooleanValue(Constants.ARGS_STREAM);

    String[] stages = taskStages.split(",");
    List<Integer> taskList = new ArrayList<>();
    for (String s : stages) {
      taskList.add(Integer.valueOf(s));
    }

    LOG.info(String.format("Starting with arguments: iter %d size %d operation %s containers "
            + "%d taskStages %s gap %d file %s outstanding %d threads %b",
        iterations, size, col, containers, taskList, gap, fName, outstanding, threads));

    JobParameters jobParameters = new JobParameters(size, iterations, col,
        containers, taskList, gap);
    jobParameters.fileName = fName;
    jobParameters.outstanding = outstanding;
    jobParameters.threads = threads;
    jobParameters.printInterval = pi;
    jobParameters.dataType = type;
    jobParameters.initIterations = intItr;
    jobParameters.doVerify = doVerify;
    jobParameters.stream = stream;
    return jobParameters;
  }

  @Override
  public String toString() {
    return "JobParameters{"
        + "size=" + size
        + ", iterations=" + iterations
        + ", col=" + operation
        + ", containers=" + containers
        + '}';
  }
}
