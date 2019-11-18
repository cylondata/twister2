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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.ComputeResourceUtils;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;

public class  DriverExample implements IDriver {
  private static final Logger LOG = Logger.getLogger(DriverExample.class.getName());

  private Object waitObject = new Object();

  @Override
  public void execute(Config config, IScaler scaler, IDriverMessenger messenger) {

    waitAllWorkersToJoin();

//    broadcastExample(messenger);
//    scalingExampleCLI(scaler);
    scalingExample(scaler, messenger);

    LOG.info("Driver has finished execution.");
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
//    LOG.info("All workers joined: " + WorkerInfoUtils.workerListAsString(workerList));
    List<Integer> ids = workerList.stream()
        .map(wi -> wi.getWorkerID())
        .collect(Collectors.toList());

    LOG.info("All workers joined. Worker IDs: " + ids);

    synchronized (waitObject) {
      waitObject.notify();
    }

  }

  /**
   * wait for all workers to join the job
   * this can be used for waiting initial worker joins or joins after scaling up the job
   */
  private void waitAllWorkersToJoin() {
    synchronized (waitObject) {
      try {
        LOG.info("Waiting for all workers to join the job... ");
        waitObject.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }
    }

  }

  private void scalingExampleCLI(IScaler scaler) {
    java.util.Scanner scanner = new java.util.Scanner(System.in);
    LOG.info("Testing scaling up and down ............................. ");

    while (true) {
      LOG.info("Enter a char (u for scaling up, d for scaling down, q for quitting): ");
      String command = scanner.nextLine();

      if ("q".equals(command)) {
        break;

      } else if ("u".equals(command)) {
        LOG.info("Enter an integer to scale up workers: ");
        String input = scanner.nextLine();
        scaler.scaleUpWorkers(Integer.parseInt(input));

      } else if ("d".equals(command)) {
        LOG.info("Enter an integer to scale down workers: ");
        String input = scanner.nextLine();
        scaler.scaleDownWorkers(Integer.parseInt(input));

      } else {
        LOG.info("Please enter either of: u, d, or q. Press enter after.");
      }
    }
  }

  private void scalingExample(IScaler scaler, IDriverMessenger messenger) {
    LOG.info("Testing scaling up and down ............................. ");

    long sleepDuration = 30 * 1000; //
    try {
      LOG.info(String.format("Sleeping %s seconds ....", sleepDuration));
      Thread.sleep(sleepDuration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    int toAdd = 10;
    LOG.info("Adding " + toAdd + " new workers.");
    scaler.scaleUpWorkers(toAdd);

    waitAllWorkersToJoin();

    try {
      LOG.info(String.format("Sleeping %s seconds ....", sleepDuration));
      Thread.sleep(sleepDuration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    int toRemove = 5;
    LOG.info("removing " + toRemove + " workers.");
    scaler.scaleDownWorkers(toRemove);

    try {
      LOG.info(String.format("Sleeping %s seconds ....", sleepDuration));
      Thread.sleep(sleepDuration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Adding " + toAdd + " new workers.");
    scaler.scaleUpWorkers(toAdd);

    waitAllWorkersToJoin();

    try {
      LOG.info(String.format("Sleeping %s seconds ....", sleepDuration));
      Thread.sleep(sleepDuration * 100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    JobMasterAPI.WorkerStateChange stateChange = JobMasterAPI.WorkerStateChange.newBuilder()
        .setState(JobMasterAPI.WorkerState.COMPLETED)
        .build();

    LOG.info("Broadcasting the message: " + stateChange);
    messenger.broadcastToAllWorkers(stateChange);
  }

  private void broadcastExample(IDriverMessenger messenger) {

    LOG.info("Testing broadcasting  ............................. ");

    // construct an example protocol buffer message and broadcast it to all workers
    JobMasterAPI.NodeInfo nodeInfo =
        NodeInfoUtils.createNodeInfo("example.nodeIP", "rack-01", "dc-01");

    LOG.info("Broadcasting an example NodeInfo protocol buffer message: " + nodeInfo);
    messenger.broadcastToAllWorkers(nodeInfo);

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    JobAPI.ComputeResource computeResource =
        ComputeResourceUtils.createComputeResource(10, 0.5, 2048, 2.0);

    LOG.info("Broadcasting an example ComputeResource protocol buffer message: " + computeResource);
    messenger.broadcastToAllWorkers(computeResource);

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void workerMessageReceived(Any anyMessage, int senderID) {
    if (anyMessage.is(JobMasterAPI.NodeInfo.class)) {
      try {
        JobMasterAPI.NodeInfo nodeInfo = anyMessage.unpack(JobMasterAPI.NodeInfo.class);
        LOG.info("Received WorkerMessage from worker: " + senderID
            + ". NodeInfo: " + nodeInfo);

      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      }
    } else if (anyMessage.is(JobAPI.ComputeResource.class)) {
      try {
        JobAPI.ComputeResource computeResource = anyMessage.unpack(JobAPI.ComputeResource.class);
        LOG.info("Received WorkerMessage from worker: " + senderID
            + ". ComputeResource: " + computeResource);

      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      }
    } else {
      LOG.info("Received WorkerMessage from worker: " + senderID
          + ". Message: " + anyMessage);

    }
  }
}
