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
package edu.iu.dsc.tws.master.dashclient;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import edu.iu.dsc.tws.master.dashclient.messages.JobStateChange;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterJob;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterWorker;
import edu.iu.dsc.tws.master.dashclient.messages.ScaleComputeResource;
import edu.iu.dsc.tws.master.dashclient.messages.WorkerStateChange;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This client is used to send messages to Dashboard by JobMaster
 *
 */

public class DashboardClient {
  private static final Logger LOG = Logger.getLogger(DashboardClient.class.getName());

  private String dashHost;
  private String jobID;

  public DashboardClient(String dashHost, String jobID) {
    this.dashHost = dashHost;
    this.jobID = jobID;
  }

  /**
   * send registerJob message to Dashboard
   * when a job master starts, it sends this message to Dashboard
   * @param job
   * @param jobMasterNodeInfo
   * @return
   */
  public boolean registerJob(JobAPI.Job job, JobMasterAPI.NodeInfo jobMasterNodeInfo) {

    RegisterJob registerJob = new RegisterJob(jobID, job, jobMasterNodeInfo);
    String path = "jobs/";

    try {
      Response response = ClientBuilder.newClient()
          .target(dashHost)
          .path(path)
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(registerJob));

      if (response.getStatus() == 200) {
        LOG.info("Registered JobMaster with Dashboard. jobID: " + jobID);
        return true;
      } else {
        LOG.severe("Could not register JobMaster with Dashboard for jobID: " + jobID
            + ". Response: " + response.toString());
        return false;
      }
    } catch (javax.ws.rs.ProcessingException pe) {
      if (pe.getCause() instanceof java.net.ConnectException) {
        LOG.log(Level.SEVERE, "Could not connect to Dashboard at: " + dashHost, pe);
        return false;
      }

      LOG.log(Level.SEVERE, "Could not register the job with Dashboard at: " + dashHost, pe);
      return false;
    }
  }

  /**
   * send JobStateChange message to Dashboard
   * @param state
   * @return
   */
  public boolean jobStateChange(JobState state) {
    JobStateChange jobStateChange = new JobStateChange(state.name());

    String path = "jobs/" + jobID + "/state/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(jobStateChange));

    if (response.getStatus() == 200) {
      LOG.info("Job " + state.name() + " message sent to Dashboard successfully.");
      return true;
    } else {
      LOG.severe("Job " + state.name() + " message could not be sent to Dashboard. Response: "
          + response.toString());
      return false;
    }
  }

  /**
   * send ScaleComputeResource message to Dashboard
   * instances shows the updated value of the instances for this compute resource
   * instances may be smaller or higher than the original value
   * if it is smaller, it means some instances of that resource removed
   * if it is higher, it means some instances of that resource added
   * @return
   */
  public boolean scaleComputeResource(int computeResourceIndex, int instances) {

    ScaleComputeResource scaleComputeResource = new ScaleComputeResource(instances);

    String path = "jobs/" + jobID + "/computeResources/" + computeResourceIndex + "/scale/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(scaleComputeResource));

    if (response.getStatus() == 200) {
      LOG.info("ScaleComputeResource message sent to Dashboard successfully."
          + " computeResourceIndex: " + computeResourceIndex
          + " instances new value: " + instances);
      return true;
    } else {
      LOG.severe("ScaleComputeResource message could not be sent to Dashboard. Response: "
          + response.toString());
      return false;
    }
  }

  /**
   * send RegisterWorker message to Dashboard
   */
  public boolean registerWorker(JobMasterAPI.WorkerInfo workerInfo) {

    RegisterWorker registerWorker = new RegisterWorker(jobID, workerInfo);
    String path = "workers/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(registerWorker));

    if (response.getStatus() == 200) {
      LOG.info("Registered Worker with Dashboard successfully "
          + "for workerID: " + workerInfo.getWorkerID());
      return true;
    } else {
      LOG.severe("Sending RegisterWorker message to Dashboard is unsuccessful "
          + "for workerID: " + workerInfo.getWorkerID() + ". Response: " + response.toString());
      return false;
    }
  }

  /**
   * send HeartBeat message to Dashboard for the given worker
   * @param workerID
   * @return
   */
  public boolean workerHeartbeat(int workerID) {
    String path = "workers/" + jobID + "/" + workerID + "/beat/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(""));

    if (response.getStatus() == 200) {
      LOG.fine("Sent HeartBeat message to Dashboard successfully for workerID: " + workerID);
      return true;
    } else {
      LOG.severe("Sending HeartBeat message to Dashboard is unsuccessful. "
          + "for workerID: " + workerID + " Response: " + response.toString());
      return false;
    }
  }

  /**
   * send WorkerStateChange message to Dashboard
   * @param workerID
   * @param state
   * @return
   */
  public boolean workerStateChange(int workerID, JobMasterAPI.WorkerState state) {
    WorkerStateChange workerStateChange = new WorkerStateChange(state.name());

    String path = "workers/" + jobID + "/" + workerID + "/state/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(workerStateChange));

    if (response.getStatus() == 200) {
      LOG.info("Sent Worker " + state.name() + " message to Dashboard successfully "
          + "for workerID: " + workerID);
      return true;
    } else {
      LOG.severe("Sending Worker " + state.name() + " message to Dashboard is unsuccessful "
          + "for workerID: " + workerID + " Response: " + response.toString());
      return false;
    }
  }

}
