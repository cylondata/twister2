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

import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.client.JMWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class Twister2HTGSubmitter {

  private static final Logger LOG = Logger.getLogger(Twister2HTGSubmitter.class.getName());

  private Config config;

  public Twister2HTGSubmitter(Config cfg) {
    this.config = cfg;
  }

  /**
   * execute method to initiate the job submission
   */
  public void execute(Twister2Metagraph twister2Metagraph,
                      JobConfig jobConfig,
                      String workerclassName) {

    LOG.info("HTG Sub Graph Requirements:" + twister2Metagraph.getSubGraph()
        + "\nHTG Relationship Values:" + twister2Metagraph.getRelation());

    HTGJobAPI.ExecuteMessage executeMessage = null;
    Twister2Metagraph.SubGraph subGraph = null;

    //Call the schedule method to identify the graph to be executed
    List<String> scheduleGraphs = schedule(twister2Metagraph);
    for (int i = 0; i < scheduleGraphs.size(); i++) {
      String subgraphName = scheduleGraphs.get(i);
      subGraph = twister2Metagraph.getMetaGraphMap(subgraphName);

      //Set the subgraph to be executed from the metagraph
      executeMessage = HTGJobAPI.ExecuteMessage.newBuilder()
          .setSubgraphName(subgraphName)
          .build();
    }

    //Construct the HTGJob object to be sent to Job Master
    HTGJobAPI.HTGJob htgJob = HTGJobAPI.HTGJob.newBuilder()
        .setHtgJobname(twister2Metagraph.getHTGName())
        .addAllGraphs(twister2Metagraph.getSubGraph())
        .addAllRelations(twister2Metagraph.getRelation())
        .build();

    //TODO:Just for the reference to the job master (this statement could be removed)
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(htgJob.getHtgJobname())
        .setWorkerClass(workerclassName)
        .addComputeResource(subGraph.getCpu(), subGraph.getRamMegaBytes(),
            subGraph.getDiskGigaBytes(), subGraph.getNumberOfInstances())
        .setConfig(jobConfig)
        .build();

    //Submit the complete job using Twister2 Submitter
    submitJob(subGraph, config, jobConfig, workerclassName);

    //Send the HTG Job information to execute the part of the HTG
    submitToJobMaster(htgJob, executeMessage, twister2Job);
  }

  /**
   * This schedule is the base method for making decisions to run the part of the task graph which
   * will be improved further with the complex logic. Now, based on the relations(parent -> child)
   * it will initiate the execution.
   */
  private List<String> schedule(Twister2Metagraph twister2Metagraph) {

    List<String> scheduledGraph = new LinkedList<>();

    for (int i = 0; i < twister2Metagraph.getRelation().size(); i++) {
      ((LinkedList<String>) scheduledGraph).addFirst(
          twister2Metagraph.getRelation().iterator().next().getParent());
      scheduledGraph.addAll(Collections.singleton(
          twister2Metagraph.getRelation().iterator().next().getChild()));
    }
    LOG.info("Scheduled Graph list details:" + scheduledGraph);
    return scheduledGraph;
  }

  private void submitJob(Twister2Metagraph.SubGraph subGraph,
                         Config cfg, JobConfig jobConfig, String className) {

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(subGraph.getName())
        .setWorkerClass(className)
        .addComputeResource(subGraph.getCpu(), subGraph.getRamMegaBytes(),
            subGraph.getDiskGigaBytes(), subGraph.getNumberOfInstances())
        .setConfig(jobConfig)
        .build();

    // now submit the job
    //Twister2Submitter.submitJob(twister2Job, cfg);
  }

  /**
   * This method is to submit the htg job to the job master.
   */
  public void submitToJobMaster(HTGJobAPI.HTGJob htgJob,
                                HTGJobAPI.ExecuteMessage executeMessage,
                                Twister2Job twister2Job) {

    JobAPI.Job job = twister2Job.serialize();

    //TODO:Starting Job Master for validation (It would be removed)
    JobMaster jobMaster = new JobMaster(config, "localhost", null, job, null);
    jobMaster.startJobMasterThreaded();

    LOG.fine("HTG job to send jobmaster:::::::::::" + htgJob.getGraphsList()
        + "\t" + htgJob.getRelationsList() + "\t" + executeMessage.getSubgraphName());

    InetAddress htgClientIP = JMWorkerController.convertStringToIP("localhost");
    int htgClientPort = 10000 + (int) (Math.random() * 10000);

    int htgClientTempID = 0;

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo(
        "htg.client.ip", "rack01", null);
    JobMasterAPI.HTGClientInfo htgClientInfo = HTGClientInfoUtils.createHTGClientInfo(
        htgClientTempID, htgClientIP.getHostAddress(), htgClientPort, nodeInfo);

    Twister2HTGClient client = new Twister2HTGClient(config, htgClientInfo, htgJob, executeMessage);
    Thread clientThread = client.startThreaded();
    if (clientThread == null) {
      LOG.severe("HTG Client can not initialize. Exiting ...");
      return;
    }

    IWorkerController workerController = client.getJMWorkerController();

    client.sendHTGClientStartingMessage();

    // wait up to 2sec
    sleep((long) (Math.random() * 2000));

    client.sendHTGClientRunningMessage();

    client.close();

    LOG.info("HTG Client finished the connection");
  }

  public static void sleep(long duration) {

    LOG.info("Sleeping " + duration + "ms............");

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static class HTGClientInfoUtils {
    public static JobMasterAPI.HTGClientInfo createHTGClientInfo(int htgClientID,
                                                                 String hostAddress,
                                                                 int htgClientPort,
                                                                 JobMasterAPI.NodeInfo nodeInfo) {

      JobMasterAPI.HTGClientInfo.Builder builder = JobMasterAPI.HTGClientInfo.newBuilder();
      builder.setClientID(htgClientID);
      builder.setClientIP(hostAddress);
      builder.setPort(htgClientPort);

      if (nodeInfo != null) {
        builder.setNodeInfo(nodeInfo);
      }

      return builder.build();
    }
  }
}
