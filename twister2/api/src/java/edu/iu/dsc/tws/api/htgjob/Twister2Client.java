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

import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public final class Twister2Client {

  private static final Logger LOG = Logger.getLogger(Twister2Client.class.getName());

  private Twister2Client() {
  }

  /**
   * This is the execute method to invoke the submission of job
   */
  public static void execute(Twister2Metagraph twister2Metagraph,
                             Config config, JobConfig jobConfig,
                             String workerclassName) {

    LOG.info("HTG Sub Graphs:" + twister2Metagraph.getSubGraph()
        + "\nHTG Relationship Values:" + twister2Metagraph.getRelation());

    Set<HTGJobAPI.Relation> relationSet = twister2Metagraph.getRelation();

    String parent = null;
    String child = null;
    String operation = null;

    for (HTGJobAPI.Relation relation : relationSet) {
      parent = relation.getParent();
      child = relation.getChild();
      operation = relation.getOperation();
      LOG.info("Relation values:" + parent + "\t" + child + "\t" + operation);
    }

    //To retrieve the subgraph set values
    Set<HTGJobAPI.SubGraph> subGraphSet = twister2Metagraph.getSubGraph();
    Twister2Metagraph.SubGraph subGraph = null;
    int i = 1;
    for (HTGJobAPI.SubGraph subgraph : subGraphSet) {
      //subGraph = twister2Metagraph.getMetaGraphMap(subgraph.getName());
      subGraph = twister2Metagraph.getMetaGraphMap(parent);
      LOG.info("Sub Graph" + i + "\tand Resource Requirements:" + subGraph.getName() + "\t"
          + subGraph.getCpu() + "\t" + subGraph.getDiskGigaBytes() + "\t"
          + subGraph.getRamMegaBytes() + "\t" + subGraph.getNumberOfInstances() + "\t"
          + subGraph.getWorkersPerPod());
      ++i;
      twister2Metagraph.setExecuteMessage(subGraph.getName());
    }

    LOG.info("Execute Message:" + twister2Metagraph.getExecuteMessage());

    submitJob(subGraph, config, jobConfig, workerclassName);

    //Submit to Job Master
    submitToJobMaster(twister2Metagraph);

    /*Twister2MetaGraph.SubGraph subGraph1 = twister2Metagraph.getMetaGraphMap("sourcetaskgraph");
    LOG.info("Sub Graph 1 and Resource Requirements:" + subGraph1.getName() + "\t"
        + subGraph1.getCpu() + "\t" + subGraph1.getDiskGigaBytes() + "\t"
        + subGraph1.getRamMegaBytes() + "\t" + subGraph1.getNumberOfInstances() + "\t"
        + subGraph1.getWorkersPerPod());

    Twister2MetaGraph.SubGraph subGraph2 = twister2Metagraph.getMetaGraphMap("sinktaskgraph");
    LOG.info("Sub Graph 2 and Resource Requirements:" + subGraph2.getName() + "\t"
        + subGraph2.getCpu() + "\t" + subGraph2.getDiskGigaBytes() + "\t"
        + subGraph2.getRamMegaBytes() + "\t" + subGraph2.getNumberOfInstances() + "\t"
        + subGraph2.getWorkersPerPod());

    twister2Metagraph.setExecuteMessage(subGraph1.getName());
    submitJob(subGraph1, config, jobConfig, workerclassName);*/

  }

  public static void submitToJobMaster(Twister2Metagraph twister2MetaGraph) {
  }

  private static void submitJob(Twister2Metagraph.SubGraph subGraph,
                                Config config, JobConfig jobConfig, String className) {

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(subGraph.getName())
        .setWorkerClass(className)
        .addComputeResource(subGraph.getCpu(), subGraph.getRamMegaBytes(),
            subGraph.getDiskGigaBytes(), subGraph.getNumberOfInstances())
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}

