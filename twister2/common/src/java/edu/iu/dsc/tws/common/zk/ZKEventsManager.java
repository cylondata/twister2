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
package edu.iu.dsc.tws.common.zk;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public final class ZKEventsManager {
  public static final Logger LOG = Logger.getLogger(ZKEventsManager.class.getName());

  private static int eventCounter = 0;

  private ZKEventsManager() {
  }

  /**
   * Create job znode for persistent states
   * Assumes that there is no znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void createEventsZNode(CuratorFramework client, String rootPath, String jobID) {

    String eventsDir = ZKUtils.eventsDir(rootPath, jobID);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(eventsDir);

      LOG.info("Job EventsZnode created: " + eventsDir);

    } catch (Exception e) {
      throw new Twister2RuntimeException("EventsZnode can not be created for the path: "
          + eventsDir, e);
    }
  }

  public static void initEventCounter(CuratorFramework client,
                                      String rootPath,
                                      String jobID) {

    String eventsDir = ZKUtils.eventsDir(rootPath, jobID);

    try {
      eventCounter = client.getChildren().forPath(eventsDir).size();
      LOG.info("eventCounter is set to: " + eventCounter);
    } catch (Exception e) {
      throw new Twister2RuntimeException("Could not get children of events directory: "
          + eventsDir, e);
    }
  }

  /**
   * construct the next event path
   * increase the eventCounter by one
   */
  public static String constructEventPath(String rootPath, String jobID) {
    return ZKUtils.eventsDir(rootPath, jobID) + "/" + eventCounter++;
  }

  public static void publishEvent(CuratorFramework client,
                                  String rootPath,
                                  String jobID,
                                  JobMasterAPI.JobEvent jobEvent) throws Twister2Exception {

    String eventPath = constructEventPath(rootPath, jobID);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(eventPath, jobEvent.toByteArray());

      if (jobEvent.hasAllJoined()) {
        LOG.info("AllJoined JobEvent published. Number of workers: "
            + jobEvent.getAllJoined().getNumberOfWorkers());
      } else {
        LOG.info("JobEvent published: " + jobEvent);
      }
    } catch (Exception e) {
      throw new Twister2Exception("JobEvent can not be created for the path: "
          + eventPath, e);
    }
  }

  public static int getNumberOfPastEvents(CuratorFramework client,
                                          String rootPath,
                                          String jobID) throws Twister2Exception {

    String eventsDir = ZKUtils.eventsDir(rootPath, jobID);

    try {
      int numberOfPastEvents = client.getChildren().forPath(eventsDir).size();
      LOG.info("Number of past events: " + numberOfPastEvents);
      return numberOfPastEvents;
    } catch (Exception e) {
      throw new Twister2Exception("Could not get children of events directory: "
          + eventsDir, e);
    }
  }

  public static JobMasterAPI.JobEvent decodeJobEvent(byte[] eventData)
      throws InvalidProtocolBufferException {

    return JobMasterAPI.JobEvent.newBuilder()
        .mergeFrom(eventData)
        .build();
  }

  /**
   * return all registered events
   */
  public static TreeMap<Integer, JobMasterAPI.JobEvent> getAllEvents(CuratorFramework client,
                                                                     String rootPath,
                                                                     String jobID)
      throws Twister2Exception {

    String eventsDir = ZKUtils.eventsDir(rootPath, jobID);

    try {
      TreeMap<Integer, JobMasterAPI.JobEvent> events = new TreeMap<>(Collections.reverseOrder());
      List<String> children = client.getChildren().forPath(eventsDir);
      for (String childName : children) {
        String childPath = eventsDir + "/" + childName;
        int eventIndex = Integer.parseInt(childName);
        byte[] eventNodeBody = client.getData().forPath(childPath);
        JobMasterAPI.JobEvent event = decodeJobEvent(eventNodeBody);
        events.put(eventIndex, event);
      }

      return events;
    } catch (Exception e) {
      throw new Twister2Exception("Could not get event znode data: "
          + eventsDir, e);
    }
  }

}
