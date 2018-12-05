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

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class DashboardClient {

  private int n1 = 0;

  public DashboardClient() {
  }

  public static void main(String[] args) {
    testCreateWorker();
//    testWorkerStateChange();
  }

  public static void testWorkerStateChange() {
    WorkerStateChange workerStateChange = new WorkerStateChange();
    workerStateChange.setState("COMPLETED");
    int workerID = 1;
    String jobID = "job-0";

    String path = "workers/" + jobID + "/" + workerID + "/state/";

    Response response = ClientBuilder.newClient()
        .target("http://localhost:8080")
        .path(path)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(workerStateChange));

    if (response.getStatus() == 200) {
      System.out.println("WorkerStateChange is successfull. :))) ");
    } else {
      System.out.println("WorkerStateChange failed. :(((( ");
      System.out.println(response.toString());
    }

  }

  public static void testCreateWorker() {

    Node workerNode = new Node();
    workerNode.setIp("111.111.111");
    workerNode.setRack("rack-0");
    workerNode.setDataCenter("dc-0");

    RegisterWorker registerWorker = new RegisterWorker();
    registerWorker.setWorkerId(2);
    registerWorker.setWorkerIP("222.222.222");
    registerWorker.setJobId("job-0");
    registerWorker.setWorkerPort(1234);
    registerWorker.setComputeResourceIndex(0);
    registerWorker.setNode(workerNode);

    System.out.println("json message to send: \n" + Entity.json(registerWorker).toString());

    Entity<RegisterWorker> jsonEntity = Entity.json(registerWorker);

    Response response = ClientBuilder.newClient()
        .target("http://localhost:8080")
        .path("workers/")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(registerWorker));

    if (response.getStatus() == 200) {
      System.out.println("RegisterWorker is successfull. :))) ");
    } else {
      System.out.println("RegisterWorker failed. :(((( ");
      System.out.println(response.toString());
    }
  }



  public static void test1(String[] args) {
    Node node = ClientBuilder.newClient()
        .target("http://localhost:8080")
        .path("nodes")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get(Node.class);

    System.out.println("Response received: ");
    System.out.println(node);
  }

}
