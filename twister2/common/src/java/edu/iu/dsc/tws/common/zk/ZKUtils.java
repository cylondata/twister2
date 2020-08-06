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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;

/**
 * this class provides methods to construct znode path names for jobs and workers
 * in addition, it provides methods to cleanup znodes on zookeeper server
 */
public final class ZKUtils {
  public static final Logger LOG = Logger.getLogger(ZKUtils.class.getName());

  // a singleton client
  private static CuratorFramework client;

  private ZKUtils() {
  }

  public static CuratorFramework getClient() {
    return client;
  }

  /**
   * connect to ZooKeeper server
   * @param zkServers
   * @return
   */
  public static CuratorFramework connectToServer(String zkServers) {
    return connectToServer(zkServers, FaultToleranceContext.FAILURE_TIMEOUT_DEFAULT);
  }

  /**
   * connect to ZooKeeper server
   */
  public static synchronized CuratorFramework connectToServer(String zkServers,
                                                              int sessionTimeoutMs) {

    if (client != null) {
      return client;
    }

    try {
      int connectionTimeoutMs = sessionTimeoutMs;
      client = CuratorFrameworkFactory.newClient(zkServers,
          sessionTimeoutMs, connectionTimeoutMs, new ExponentialBackoffRetry(1000, 5));
      client.start();

      LOG.log(Level.FINE, "Connected to ZooKeeper server: " + zkServers);
      return client;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not connect to ZooKeeper server" + zkServers, e);
      throw new RuntimeException(e);
    }
  }

  public static void closeClient() {
    if (client != null) {
      CloseableUtils.closeQuietly(client);
      client = null;
    }
  }

  /**
   * construct main job directory path for the job
   */
  public static String jobDir(String rootPath, String jobID) {
    return rootPath + "/" + jobID;
  }

  /**
   * construct ephemeral directory path for the job
   */
  public static String ephemDir(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/workers-ephem-state";
  }

  /**
   * construct persistent directory path for the job
   */
  public static String persDir(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/workers-pers-state";
  }

  /**
   * construct events directory path for the job
   */
  public static String eventsDir(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/events";
  }

  /**
   * construct default barrier directory path for the job
   */
  public static String defaultBarrierDir(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/defaultBarrier";
  }

  /**
   * construct init barrier directory path for the job
   */
  public static String initBarrierDir(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/initBarrier";
  }

  /**
   * construct a worker path from the workers directory
   */
  public static String workerPath(String workersDir, int workerID) {
    return workersDir + "/" + workerID;
  }

  /**
   * construct the job master path for a persistent znode that will store the job master state
   */
  public static String jmPersPath(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/jm-pers-state";
  }

  /**
   * construct the job master path for an ephemeral znode that will watch JM liveness
   */
  public static String jmEphemPath(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/jm-ephem-state";
  }

  /**
   * construct the job submission time znode path
   */
  public static String jobSubmisionTimePath(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/job-submission-time";
  }

  /**
   * construct the job end time znode path
   */
  public static String jobEndTimePath(String rootPath, String jobID) {
    return jobDir(rootPath, jobID) + "/job-end-time";
  }

  /**
   * WorkerID is at the end of workerPath
   * The char "-" proceeds the workerID
   * @return
   */
  public static int getWorkerIDFromEphemPath(String workerPath) {
    String workerIDStr = workerPath.substring(workerPath.lastIndexOf("-") + 1);
    return Integer.parseInt(workerIDStr);
  }

  /**
   * WorkerID is at the end of workerPath
   * The char "/" proceeds the workerID
   * @return
   */
  public static int getWorkerIDFromPersPath(String workerPath) {
    String workerIDStr = workerPath.substring(workerPath.lastIndexOf("/") + 1);
    return Integer.parseInt(workerIDStr);
  }

  /**
   * create a PersistentNode object in the given path
   * it is ephemeral and persistent
   * it will be deleted after the worker leaves or fails
   * it will be persistent for occasional network problems
   */
  public static PersistentNode createPersistentEphemeralZnode(String path,
                                                              byte[] payload) {

    return new PersistentNode(client, CreateMode.EPHEMERAL, true, path, payload);
  }

  /**
   * create a PersistentNode object in the given path
   * it needs to be deleted explicitly, not ephemeral
   * it will be persistent for occasional network problems
   */
  public static PersistentNode createPersistentZnode(String path,
                                                     byte[] payload) {

    return new PersistentNode(client, CreateMode.PERSISTENT, true, path, payload);
  }

  /**
   * encode the given WorkerInfo object as a byte array.
   * First put the worker state as a 4 byte array to the beginning
   * resulting byte array has the state bytes and workerInfo object after that
   */
  public static byte[] encodeJobMasterZnode(String masterAddress, int state) {
    byte[] stateBytes = Ints.toByteArray(state);
    byte[] addressBytes = masterAddress.getBytes(StandardCharsets.UTF_8);

    return Bytes.concat(stateBytes, addressBytes);
  }

}
