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
package edu.iu.dsc.tws.api.cdfw;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public final class DataFlowGraph {
  // the data flow graph
  private DataFlowTaskGraph graph;

  // resources for cpu
  private int cpu;

  // the ram required
  private int ramMegaBytes;

  // the disk required
  private double diskGigaBytes;

  // number of workers requested
  private int workers;

  // the job configurations
  private JobConfig jobConfig = new JobConfig();

  // input names to this
  private List<HTGJobAPI.Input> inputs = new ArrayList<>();

  // output names to this dataflow
  private List<String> outputs = new ArrayList<>();

  private DataFlowGraph(DataFlowTaskGraph g) {
    this.graph = g;
  }

  public static DataFlowGraph newSubGraphJob(DataFlowTaskGraph g) {
    return new DataFlowGraph(g);
  }

  public DataFlowGraph setCpu(int c) {
    this.cpu = c;
    return this;
  }

  public DataFlowGraph setRamMegaBytes(int ram) {
    this.ramMegaBytes = ram;
    return this;
  }

  public DataFlowGraph setDiskGigaBytes(double disk) {
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

  public DataFlowGraph addJobConfig(JobConfig jCfg) {
    jobConfig.putAll(jCfg);
    return this;
  }

  public JobConfig getJobConfig() {
    return jobConfig;
  }

  public int getWorkers() {
    return workers;
  }

  public DataFlowGraph setWorkers(int w) {
    this.workers = w;
    return this;
  }

  public DataFlowGraph addInput(String g, String input) {
    inputs.add(HTGJobAPI.Input.newBuilder().setParentGraph(g).setName(input).build());
    return this;
  }

  public DataFlowGraph addOutput(String name) {
    outputs.add(name);
    return this;
  }

  public List<HTGJobAPI.Input> getInputs() {
    return inputs;
  }

  public List<String> getOutputs() {
    return outputs;
  }
}
