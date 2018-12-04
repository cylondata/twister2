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
package edu.iu.dsc.tws.api;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.htgjob.Twister2MetaGraph;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;

public final class Twister2HTGClient {

  private static final Logger LOG = Logger.getLogger(Twister2HTGClient.class.getName());

  private Twister2HTGClient() {
  }

  public static Twister2MetaGraph.SubGraph execute(Twister2MetaGraph twister2Metagraph,
                                                   Config config, String workerclassName) {

    LOG.info("HTG Sub Graph Requirements:" + twister2Metagraph.getSubGraph()
        + "\nHTG Relationship Values:" + twister2Metagraph.getRelation());

    Twister2MetaGraph.SubGraph subGraph1 = twister2Metagraph.getMetaGraphMap("subgraph1");
    LOG.info("Sub Graph 1 and Resource Requirements:" + subGraph1.getName() + "\t"
        + subGraph1.getCpu() + "\t" + subGraph1.getDiskGigaBytes() + "\t"
        + subGraph1.getRamMegaBytes() + "\t" + subGraph1.getNumberOfInstances() + "\t"
        + subGraph1.getWorkersPerPod());

    Twister2MetaGraph.SubGraph subGraph2 = twister2Metagraph.getMetaGraphMap("subgraph2");
    LOG.info("Sub Graph 2 and Resource Requirements:" + subGraph2.getName() + "\t"
        + subGraph2.getCpu() + "\t" + subGraph2.getDiskGigaBytes() + "\t"
        + subGraph2.getRamMegaBytes() + "\t" + subGraph2.getNumberOfInstances() + "\t"
        + subGraph2.getWorkersPerPod());

    submitJob(twister2Metagraph, subGraph1, config, workerclassName);

    return subGraph1;
  }

  public static String submitJob(Twister2MetaGraph twister2Metagraph,
                                 Twister2MetaGraph.SubGraph subGraph,
                                 Config config, String workerclassName) {

    //TODO:Invoke HTG Client and send the metagraph -> start with FIFO
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(subGraph.getName());
    jobBuilder.setWorkerClass(workerclassName);
    //jobBuilder.setConfig(subGraph.getConfig());
    jobBuilder.addComputeResource(subGraph.getCpu(), subGraph.getRamMegaBytes(),
        subGraph.getDiskGigaBytes(), subGraph.getNumberOfInstances());

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);

    return "Successfully submitted";
  }
}
