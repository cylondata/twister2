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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public final class ZKEphemStateManager {
  public static final Logger LOG = Logger.getLogger(ZKEphemStateManager.class.getName());

  public static final String DELETE_TAG = "DELETED_BY_RESTARTING_WORKER";

  private ZKEphemStateManager() {
  }

  /**
   * create parent directory for ephemeral worker znodes
   */
  public static void createEphemDir(CuratorFramework client, String rootPath, String jobID) {

    String ephemDirPath = ZKUtils.ephemDir(rootPath, jobID);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(ephemDirPath);

      LOG.info("Job EphemStateDir created: " + ephemDirPath);

    } catch (Exception e) {
      throw new Twister2RuntimeException("EphemStateDir can not be created for the path: "
          + ephemDirPath, e);
    }
  }

  public static PersistentNode createWorkerZnode(CuratorFramework client,
                                                 String rootPath,
                                                 String jobID,
                                                 int workerID) {

    String ephemDirPath = ZKUtils.ephemDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(ephemDirPath, workerID);
    byte[] znodeBody = ("" + workerID).getBytes(StandardCharsets.UTF_8);

    // it is ephemeral and persistent
    // ephemeral: it will be deleted after the worker leaves or fails
    // persistent: it will be persistent for occasional network problems

    return new PersistentNode(client, CreateMode.PERSISTENT, true, workerPath, znodeBody);
  }

  /**
   * remove ephemeral worker znode from previous run if exist
   */
  public static void removeEphemZNode(CuratorFramework client,
                                      String rootPath,
                                      String jobID,
                                      int workerID) throws Twister2Exception {
    String ephemDirPath = ZKUtils.ephemDir(rootPath, jobID);

    try {
      List<String> children = client.getChildren().forPath(ephemDirPath);
      for (String childZnodeName : children) {
        int wID = ZKUtils.getWorkerIDFromEphemPath(childZnodeName);
        if (wID == workerID) {
          String wPath = ephemDirPath + "/" + childZnodeName;
          client.setData().forPath(wPath, DELETE_TAG.getBytes(StandardCharsets.UTF_8));
          client.delete().forPath(wPath);
          LOG.info("EphemeralWorkerZnode deleted from previous run: " + wPath);
        }
      }

    } catch (Exception e) {
      throw new Twister2Exception("Can not remove ephemeral worker znode.", e);
    }
  }

  public static String decodeWorkerZnodeBody(byte[] body) {
    return new String(body, StandardCharsets.UTF_8);
  }


}
