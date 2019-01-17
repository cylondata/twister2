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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.resource.ComputeResourceUtils;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.common.resource.WorkerInfoUtils;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class DashboardClientExample {
  private static final Logger LOG = Logger.getLogger(DashboardClientExample.class.getName());

  private DashboardClientExample() { }

  public static void main(String[] args) {

    if (args.length < 2) {
      printUsage();
      return;
    }

    String dashAddress = args[0];
    String jobID = args[1];
    DashboardClient dashClient = new DashboardClient(dashAddress, jobID);
    sendJobKilledMessage(dashClient);

//    DashboardClient dashClient = new DashboardClient("http://localhost:8080", "job-1");

//    testRegisterJob(dashClient);
//    testRegisterWorker(dashClient);

    // test state change
//    dashClient.jobStateChange(JobState.STARTED);
//    dashClient.workerStateChange(0, JobMasterAPI.WorkerState.RUNNING);
  }

  public static void sendJobKilledMessage(DashboardClient dashClient) {

    dashClient.jobStateChange(JobState.KILLED);

  }

  public static void testRegisterJob(DashboardClient dashClient) {

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("job-1")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(2, 1024, 5)
        .addComputeResource(1, 512, 3)
        .build();

    JobAPI.Job job = twister2Job.serialize();

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo("123.123.123", "rack-0", "dc-0");

    dashClient.registerJob(job, nodeInfo);
  }

  public static void testRegisterWorker(DashboardClient dashClient) {

    JobAPI.ComputeResource computeResource =
        ComputeResourceUtils.createComputeResource(0, 1, 1024, 1);

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo("123.123.123", "rack-0", "dc-0");

    int workerID = 0;
    JobMasterAPI.WorkerInfo workerInfo =
        WorkerInfoUtils.createWorkerInfo(workerID, "123.456.789", 9009, nodeInfo, computeResource);

    dashClient.registerWorker(workerInfo);
  }

  public static void printUsage() {
    LOG.info("Usage: java edu.iu.dsc.tws.examples.internal.jobmaster.DashboardClientExample "
        + "dashAddress jobID"
        + "\n sends job KILLED message to Dashboard for this job.");
  }
}
