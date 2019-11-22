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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import edu.iu.dsc.tws.master.dashclient.messages.JobStateChange;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterJob;
import edu.iu.dsc.tws.master.dashclient.messages.RegisterWorker;
import edu.iu.dsc.tws.master.dashclient.messages.ScaledWorkers;
import edu.iu.dsc.tws.master.dashclient.messages.WorkerStateChange;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This client is used to send messages to Dashboard by JobMaster
 */

public class DashboardClient {
  private static final Logger LOG = Logger.getLogger(DashboardClient.class.getName());

  private String dashHost;
  private String jobID;
  private PoolingHttpClientConnectionManager poolingConnManager;
  private LinkedBlockingQueue<CloseableHttpClient> httpClientQueue;

  private int numberOfConnections = 3;
  private ObjectMapper mapper;

  public DashboardClient(String dashHost, String jobID, int numberOfConnections) {
    this.dashHost = dashHost;
    this.jobID = jobID;
    this.numberOfConnections = numberOfConnections;
    this.httpClientQueue = new LinkedBlockingQueue<>();

    poolingConnManager = new PoolingHttpClientConnectionManager();
    poolingConnManager.setMaxTotal(numberOfConnections);
    poolingConnManager.setDefaultMaxPerRoute(numberOfConnections);
    HttpHost httpHost = new HttpHost(dashHost);
    poolingConnManager.setMaxPerRoute(new HttpRoute(httpHost), numberOfConnections);

    for (int i = 0; i < numberOfConnections; i++) {
      httpClientQueue.add(HttpClients.custom().setConnectionManager(poolingConnManager).build());
    }

    mapper = new ObjectMapper();
  }

  /**
   * return next CloseableHttpClient
   * wait until the next one becomes available
   */
  private CloseableHttpClient getHttpClient() {
    CloseableHttpClient httpClient = null;
    while ((httpClient = httpClientQueue.poll()) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warning("Thread sleep interrupted.");
      }
    }
    return httpClient;
  }

  private void putHttpClient(CloseableHttpClient httpClient) {
    try {
      httpClientQueue.put(httpClient);
    } catch (InterruptedException e) {
      LOG.warning(e.getMessage());
    }
  }

  private HttpPost constructHttpPost(String endPoint, String jsonStr) {
    HttpPost httpPost = new HttpPost(endPoint);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    StringEntity entity = null;
    try {
      entity = new StringEntity(jsonStr);
    } catch (UnsupportedEncodingException e) {
      LOG.log(Level.SEVERE, "Could not create StringEntity from json string.", e);
      return null;
    }
    httpPost.setEntity(entity);
    return httpPost;
  }

  /**
   * send registerJob message to Dashboard
   * when a job master starts, it sends this message to Dashboard
   */
  public boolean registerJob(JobAPI.Job job, JobMasterAPI.NodeInfo jobMasterNodeInfo) {

    RegisterJob registerJob = new RegisterJob(jobID, job, jobMasterNodeInfo);
    LOG.fine("Registering job to dashboard: " + job + "jobMasterNodeInfo: " + jobMasterNodeInfo);
    String endPoint = dashHost + "/jobs/";

    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(registerJob);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert java entity object to Json string.", e);
      return false;
    }

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.info("Registered JobMaster with Dashboard. jobID: " + jobID);
        return true;
      } else {
        LOG.severe("Could not register JobMaster with Dashboard for jobID: " + jobID
            + ". Response: " + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  /**
   * send JobStateChange message to Dashboard
   */
  public boolean jobStateChange(JobState state) {
    JobStateChange jobStateChange = new JobStateChange(state.name());

    String endPoint = dashHost + "/jobs/" + jobID + "/state/";

    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(jobStateChange);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert java entity object to Json string.", e);
      return false;
    }

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.info("Job " + state.name() + " message sent to Dashboard successfully.");
        return true;
      } else {
        LOG.severe("Job " + state.name() + " message could not be sent to Dashboard. Response: "
            + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  /**
   * send ScaleComputeResource message to Dashboard
   * instances shows the updated value of the instances for this compute resource
   * instances may be smaller or higher than the original value
   * if it is smaller, it means some instances of that resource removed
   * if it is higher, it means some instances of that resource added
   */
  public boolean scaledWorkers(int change, int numberOfWorkers, List<Integer> killedWorkers) {

    ScaledWorkers scaledWorkers = new ScaledWorkers(change, numberOfWorkers, killedWorkers);
    String endPoint = dashHost + "/jobs/" + jobID + "/scale/";

    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(scaledWorkers);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert java entity object to Json string.", e);
      return false;
    }

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.info("ScaledWorkers message sent to Dashboard successfully."
            + " change: " + change
            + " numberOfWorkers: " + numberOfWorkers);
        return true;
      } else {
        LOG.severe("ScaledWorkers message could not be sent to Dashboard. Response: "
            + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  /**
   * send RegisterWorker message to Dashboard
   * initialState must be either STARTED or RESTARTED
   */
  public boolean registerWorker(JobMasterAPI.WorkerInfo workerInfo,
                                JobMasterAPI.WorkerState initialState) {

    RegisterWorker registerWorker = new RegisterWorker(jobID, workerInfo, initialState);
    String endPoint = dashHost + "/workers/";

    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(registerWorker);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert java entity object to Json string.", e);
      return false;
    }

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.info("Registered Worker with Dashboard successfully "
            + "for workerID: " + workerInfo.getWorkerID());
        return true;
      } else {
        LOG.severe("Sending RegisterWorker message to Dashboard is unsuccessful "
            + "for workerID: " + workerInfo.getWorkerID() + ". Response: " + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  /**
   * send HeartBeat message to Dashboard for the given worker
   */
  public boolean workerHeartbeat(int workerID) {
    String endPoint = dashHost + "/workers/" + jobID + "/" + workerID + "/beat/";

    String jsonStr = "";

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.fine("Sent HeartBeat message to Dashboard successfully for workerID: " + workerID);
        return true;
      } else {
        LOG.severe("Sending HeartBeat message to Dashboard is unsuccessful. "
            + "for workerID: " + workerID + " Response: " + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  /**
   * send WorkerStateChange message to Dashboard
   */
  public boolean workerStateChange(int workerID, JobMasterAPI.WorkerState state) {
    WorkerStateChange workerStateChange = new WorkerStateChange(state.name());
    String endPoint = dashHost + "/workers/" + jobID + "/" + workerID + "/state/";

    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(workerStateChange);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert java entity object to Json string.", e);
      return false;
    }

    HttpPost httpPost = constructHttpPost(endPoint, jsonStr);
    if (httpPost == null) {
      return false;
    }

    try {
      CloseableHttpClient httpClient = getHttpClient();
      HttpResponse response = httpClient.execute(httpPost);
      EntityUtils.consume(response.getEntity());
      putHttpClient(httpClient);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.info("Sent Worker " + state.name() + " message to Dashboard successfully "
            + "for workerID: " + workerID);
        return true;
      } else {
        LOG.severe("Sending Worker " + state.name() + " message to Dashboard is unsuccessful "
            + "for workerID: " + workerID + " Response: " + response.toString());
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not execute Http Request.", e);
      return false;
    }
  }

  public void close() {
    poolingConnManager.close();
    while (httpClientQueue.isEmpty()) {
      try {
        httpClientQueue.poll().close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
    }
  }

}
