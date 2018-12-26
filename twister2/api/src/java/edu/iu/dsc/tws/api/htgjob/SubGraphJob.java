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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public final class SubGraphJob {
  private DataFlowTaskGraph graph;

  private int cpu;

  private int ramMegaBytes;

  private double diskGigaBytes;

  private int workers;

  private JobConfig jobConfig = new JobConfig();

  private SubGraphJob(DataFlowTaskGraph g) {
    this.graph = g;
  }

  public static SubGraphJob newSubGraphJob(DataFlowTaskGraph g) {
    return new SubGraphJob(g);
  }

  public SubGraphJob setCpu(int c) {
    this.cpu = c;
    return this;
  }

  public SubGraphJob setRamMegaBytes(int ram) {
    this.ramMegaBytes = ram;
    return this;
  }

  public SubGraphJob setDiskGigaBytes(double disk) {
    this.diskGigaBytes = disk;
    return this;
  }

  public DataFlowTaskGraph getGraph() {
    return graph;
  }

  public int getCpu() {
    return cpu;
  }

  public int getRamMegaBytes() {
    return ramMegaBytes;
  }

  public double getDiskGigaBytes() {
    return diskGigaBytes;
  }

  public SubGraphJob addJobConfig(JobConfig jCfg) {
    jobConfig.putAll(jCfg);
    return this;
  }

  public JobConfig getJobConfig() {
    return jobConfig;
  }

  public int getWorkers() {
    return workers;
  }

  public SubGraphJob setWorkers(int w) {
    this.workers = w;
    return this;
  }
}
