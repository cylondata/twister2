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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public final class Twister2MetaGraph {

  private static final Logger LOG = Logger.getLogger(Twister2MetaGraph.class.getName());

  private Twister2MetaGraph twister2Metagraph;

  public static Map<String, SubGraph> metaGraphMap = new HashMap<>();

  private ConnectionMode connectionMode;

  private String htgJobName;
  private JobConfig config;

  public String getExecuteMessage() {
    return executeMessage;
  }

  public void setExecuteMessage(String executeMessage) {
    this.executeMessage = executeMessage;
  }

  private String executeMessage;

  private Set<HTGJobAPI.Relation> relations = new HashSet<>();
  private Set<HTGJobAPI.SubGraph> subGraphs = new HashSet<>();

  public Twister2MetaGraph getTwister2Metagraph() {
    return twister2Metagraph;
  }

  public void setTwister2Metagraph(Twister2MetaGraph twister2MetaGraph) {
    twister2Metagraph = twister2MetaGraph;
  }

  public SubGraph getMetaGraphMap(String name) {
    return metaGraphMap.get(name);
  }

  public void setMetaGraphMap(Map<String, SubGraph> metagraphMap) {
    metaGraphMap = metagraphMap;
  }

  public Twister2MetaGraph setHTGName(String htgName) {
    this.htgJobName = htgName;
    return this;
  }

  public String getHTGName() {
    return this.htgJobName;
  }

  public Twister2MetaGraph setConnectionMode(ConnectionMode connectionmode) {
    this.connectionMode = connectionmode;
    return this;
  }

  public ConnectionMode getConnectionMode() {
    return this.connectionMode;
  }

  public Twister2MetaGraph setConfig(JobConfig jobconfig) {
    this.config = jobconfig;
    return this;
  }

  public void addSubGraph(String name, SubGraph subGraph) {

    metaGraphMap.put(name, subGraph);

    addSubGraphs(subGraph.getCpu(), subGraph.getRamMegaBytes(), subGraph.getDiskGigaBytes(),
        subGraph.getNumberOfInstances(), subGraph.getWorkersPerPod(), name);
  }

  public void addSubGraphs(double cpu, int ramMegaBytes,
                           double diskGigaBytes, int numberOfInstances,
                           int workersPerPod, String name) {
    HTGJobAPI.SubGraph subGraph = HTGJobAPI.SubGraph.newBuilder()
        .setCpu(cpu)
        .setRamMegaBytes(ramMegaBytes)
        .setDiskGigaBytes(diskGigaBytes)
        .setInstances(numberOfInstances)
        .setWorkersPerPod(workersPerPod)
        .setName(name)
        .build();

    subGraphs.add(subGraph);
  }

  public Set<HTGJobAPI.SubGraph> getSubGraph() {
    return subGraphs;
  }

  public void addRelation(String subGraph2, String subGraph1, Relation relation) {

    HTGJobAPI.Relation htRelations = HTGJobAPI.Relation.newBuilder()
        .setParent(subGraph2)
        .setChild(subGraph1)
        .setOperation(relation.getOperation())
        .build();

    relations.add(htRelations);
  }

  public Set<HTGJobAPI.Relation> getRelation() {
    return relations;
  }

  public static final class Relation {

    private String name;
    private String operation;

    private Map<String, Object> properties = new HashMap<>();

    public Relation(String name, String operation) {
      this.name = name;
      this.operation = operation;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getOperation() {
      return operation;
    }

    public void setOperation(String operation) {
      this.operation = operation;
    }

    public void addProperty(String key, Object value) {
      properties.put(key, value);
    }

    public Object getProperty(String key) {
      return properties.get(key);
    }

    public void addProperties(Map<String, Object> props) {
      this.properties.putAll(props);
    }
  }

  public static final class SubGraph {

    private String name;
    private String operation;

    private double cpu = 0.0;
    private int ramMegaBytes = 1;
    private double diskGigaBytes = 1.0;
    private int numberOfInstances = 2;
    private int workersPerPod = 1;

    private Map<String, Object> properties = new HashMap<>();

    public SubGraph(String graphname, double cpuvalue, int ramMegabytes, double diskGigabytes,
                    int numberOfinstances, int workersPerpod) {
      this.name = graphname;
      this.cpu = cpuvalue;
      this.ramMegaBytes = ramMegabytes;
      this.diskGigaBytes = diskGigabytes;
      this.numberOfInstances = numberOfinstances;
      this.workersPerPod = workersPerpod;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getOperation() {
      return operation;
    }

    public void setOperation(String operation) {
      this.operation = operation;
    }

    public double getCpu() {
      return cpu;
    }

    public void setCpu(double cpu) {
      this.cpu = cpu;
    }

    public int getRamMegaBytes() {
      return ramMegaBytes;
    }

    public void setRamMegaBytes(int ramMegaBytes) {
      this.ramMegaBytes = ramMegaBytes;
    }

    public double getDiskGigaBytes() {
      return diskGigaBytes;
    }

    public void setDiskGigaBytes(double diskGigaBytes) {
      this.diskGigaBytes = diskGigaBytes;
    }

    public int getNumberOfInstances() {
      return numberOfInstances;
    }

    public void setNumberOfInstances(int numberOfInstances) {
      this.numberOfInstances = numberOfInstances;
    }

    public int getWorkersPerPod() {
      return workersPerPod;
    }

    public void setWorkersPerPod(int workersPerPod) {
      this.workersPerPod = workersPerPod;
    }

    public void addProperty(String key, Object value) {
      properties.put(key, value);
    }

    public Object getProperty(String key) {
      return properties.get(key);
    }

    public void addProperties(Map<String, Object> props) {
      this.properties.putAll(props);
    }
  }
}
