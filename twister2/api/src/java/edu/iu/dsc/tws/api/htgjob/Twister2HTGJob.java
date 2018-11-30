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

import java.util.ArrayList;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class Twister2HTGJob {

  private static final Logger LOG = Logger.getLogger(Twister2HTGJob.class.getName());

  private static final KryoSerializer KRYO_SERIALIZER = new KryoSerializer();

  private JobConfig config;

  private String htgJobName;

  private HTGJobAPI htgJobAPI;
  private HTGJobAPI.HTGJob htGraph;

  private ArrayList<HTGJobAPI.SubGraph> htgSubGraphs = new ArrayList<>();
  private ArrayList<HTGJobAPI.Relation> htgRelations = new ArrayList<>();

  private ArrayList<JobAPI.Job> htgJobs = new ArrayList<>();

  public Twister2HTGJob() {
  }

  public HTGJobAPI.HTGJob serialize() {

    HTGJobAPI.HTGJob.Builder htgJobBuilder = HTGJobAPI.HTGJob.newBuilder();

    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();

    config.forEach((key, value) -> {
      byte[] objectByte = KRYO_SERIALIZER.serialize(value);
      configBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
    });

    htgJobBuilder.setHtgJobname(htgJobName);

    for (HTGJobAPI.SubGraph subGraph : htgSubGraphs) {
      htgJobBuilder.addGraphs(0, subGraph);
    }

    for (HTGJobAPI.Relation relation : htgRelations) {
      htgJobBuilder.addRelations(0, relation);
    }

    for (JobAPI.Job htgJob : htgJobs) {
      htgJobBuilder.setJob(htgJob);
    }

    return htgJobBuilder.build();
  }

  public static Twister2HTGMetaGraph newBuilder() {
    return new Twister2HTGMetaGraph();
  }

  public static final class Twister2HTGMetaGraph {
    private Twister2HTGJob twister2Metagraph;
    private int subGraphIndex = 0;

    public Twister2HTGMetaGraph() {
      this.twister2Metagraph = new Twister2HTGJob();
    }

    public Twister2HTGMetaGraph setHTGName(String htgName) {
      twister2Metagraph.htgJobName = htgName;
      return this;
    }

    public Twister2HTGMetaGraph addSubGraphs(double cpu, int ramMegaBytes,
                                             double diskGigaBytes, int numberOfInstances,
                                             int workersPerPod, String name) {
      HTGJobAPI.SubGraph subGraphs = HTGJobAPI.SubGraph.newBuilder()
          .setCpu(cpu)
          .setRamMegaBytes(ramMegaBytes)
          .setDiskGigaBytes(diskGigaBytes)
          .setInstances(numberOfInstances)
          .setWorkersPerPod(workersPerPod)
          .setIndex(subGraphIndex++)
          .setName(name)
          .build();

      twister2Metagraph.htgSubGraphs.add(subGraphs);
      return this;
    }

    public Twister2HTGMetaGraph addRelation(String parent, String child, String operation) {

      HTGJobAPI.Relation relations = HTGJobAPI.Relation.newBuilder()
          .setParent(parent)
          .setChild(child)
          .setOperation(operation)
          .build();

      twister2Metagraph.htgRelations.add(relations);

      return this;
    }

    public Twister2HTGMetaGraph setConfig(JobConfig config) {
      twister2Metagraph.config = config;
      return this;
    }

    public Twister2HTGJob build() {
      if (twister2Metagraph.config == null) {
        twister2Metagraph.config = new JobConfig();
      }
      return twister2Metagraph;
    }
  }

  public ArrayList<HTGJobAPI.SubGraph> getHtgSubGraphs() {
    return htgSubGraphs;
  }

  public ArrayList<HTGJobAPI.Relation> getHtgRelation() {
    return htgRelations;
  }

  private ArrayList<JobAPI.Job> getHtgJob() {
    return htgJobs;
  }
}
