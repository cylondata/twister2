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
package edu.iu.dsc.tws.executor.util;

import java.util.HashMap;

import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class ConfigUtil {

  private int threads;
  private String jobName;
  private String author;
  private String experimentId;
  private String experimentName;

  public ConfigUtil() {

  }

  public ConfigUtil(int threads) {
    this.threads = threads;
  }

  public ConfigUtil(int threads, String jobName) {
    this.threads = threads;
    this.jobName = jobName;
  }

  public ConfigUtil(int threads, String jobName, String author) {
    this.threads = threads;
    this.jobName = jobName;
    this.author = author;
  }

  public ConfigUtil(int threads, String jobName, String author, String experimentId) {
    this.threads = threads;
    this.jobName = jobName;
    this.author = author;
    this.experimentId = experimentId;
  }

  public ConfigUtil(int threads, String jobName, String author, String experimentId,
                    String experimentName) {
    this.threads = threads;
    this.jobName = jobName;
    this.author = author;
    this.experimentId = experimentId;
    this.experimentName = experimentName;
  }

  public int getThreads() {
    return threads;
  }

  public void setThreads(int threads) {
    this.threads = threads;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public void setExperimentId(String experimentId) {
    this.experimentId = experimentId;
  }

  public String getExperimentName() {
    return experimentName;
  }

  public void setExperimentName(String experimentName) {
    this.experimentName = experimentName;
  }

  @Override
  public String toString() {
    return "ConfigUtil{"
        + "threads=" + threads
        + ", jobName='" + jobName + '\''
        + ", author='" + author + '\''
        + ", experimentId='" + experimentId + '\''
        + ", experimentName='" + experimentName + '\''
        + '}';
  }

  public HashMap<String, Object> getConfigs() {
    HashMap<String, Object> configurations = new HashMap<>();
    //TODO : In future implementations add the rest of the fileds to check they are intialized

    if (this.getThreads() != 0) {
      configurations.put(SchedulerContext.THREADS_PER_WORKER, this.getThreads());
    } else {
      this.setThreads(1);
    }

    return configurations;
  }

}
