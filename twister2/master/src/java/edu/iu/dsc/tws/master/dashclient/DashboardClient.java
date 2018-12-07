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

import java.util.logging.Logger;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import edu.iu.dsc.tws.master.dashclient.messages.JobStateChange;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterJob;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterWorker;
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

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(registerJob));

    if (response.getStatus() == 200) {
      LOG.info("Registering JobMaster with Dashboard is successful");
      return true;
    } else {
      LOG.severe("Registering JobMaster with Dashboard is unsuccessful. Response: "
          + response.toString());
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
      LOG.info("Sent RegisterWorker message to Dashboard is successfully "
          + "for workerID: " + workerInfo.getWorkerID());
      return true;
    } else {
      LOG.severe("Sending RegisterWorker message to Dashboard is unsuccessful "
          + "for workerID: " + workerInfo.getWorkerID() + ". Response: " + response.toString());
      return false;
    }
  }

  /**
   * send WorkerStateChange message to Dashboard
   * @param workerID
   * @param state
   * @return
   */
  public boolean workerHeartbeat(int workerID, JobMasterAPI.WorkerState state) {
    WorkerStateChange workerStateChange = new WorkerStateChange(state.name());

    String path = "workers/" + jobID + "/" + workerID + "/state/";

    Response response = ClientBuilder.newClient()
        .target(dashHost)
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(workerStateChange));

    if (response.getStatus() == 200) {
      LOG.info("Sent WorkerStateChange message to Dashboard successfully for workerID: "
          + workerID);
      return true;
    } else {
      LOG.severe("Sending WorkerStateChange message to Dashboard is unsuccessful. "
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
