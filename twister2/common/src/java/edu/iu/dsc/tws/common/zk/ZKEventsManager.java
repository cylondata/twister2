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

import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public final class ZKEventsManager {
  public static final Logger LOG = Logger.getLogger(ZKEventsManager.class.getName());

  private static int eventCounter = 0;

  private ZKEventsManager() { }

  /**
   * Create job znode for persistent states
   * Assumes that there is no znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void createEventsZNode(CuratorFramework client, String rootPath, String jobName) {

    String eventsDir = ZKUtils.constructEventsDir(rootPath, jobName);

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
                                      String jobName) {

    String eventsDir = ZKUtils.constructEventsDir(rootPath, jobName);

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
  public static String constructEventPath(String rootPath, String jobName) {
    return ZKUtils.constructEventsDir(rootPath, jobName) + "/" + eventCounter++;
  }

  public static void publishEvent(CuratorFramework client,
                                  String rootPath,
                                  String jobName,
                                  JobMasterAPI.JobEvent jobEvent) {

    String eventPath = constructEventPath(rootPath, jobName);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(eventPath, jobEvent.toByteArray());

      LOG.info("JobEvent published: " + jobEvent);

    } catch (Exception e) {
      throw new Twister2RuntimeException("JobEvent can not be created for the path: "
          + eventPath, e);
    }
  }

}
