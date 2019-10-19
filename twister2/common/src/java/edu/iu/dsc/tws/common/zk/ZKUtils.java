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
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;

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
          sessionTimeoutMs, connectionTimeoutMs, new ExponentialBackoffRetry(1000, 3));
      client.start();

      LOG.log(Level.INFO, "Connected to ZooKeeper server: " + zkServers);
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
   * construct a job path from the given job name
   */
  public static String constructJobPath(String rootPath, String jobName) {
    return rootPath + "/" + jobName;
  }

  /**
   * construct a distributed atomic integer path for barrier
   */
  public static String constructDaiPathForBarrier(String rootPath, String jobName) {
    return rootPath + "-dai-for-barrier/" + jobName;
  }

  /**
   * construct a distributed barrier path
   */
  public static String constructBarrierPath(String rootPath, String jobName) {
    return rootPath + "-barrier/" + jobName;
  }

  /**
   * construct a worker path from the given job path and worker network info
   */
  public static String constructWorkerPath(String jobPath, int workerID) {
    return jobPath + "/" + workerID;
  }

  /**
   * WorkerID is at the end of workerPath
   * The string "w-" proceeds the workerID
   * @return
   */
  public static int getWorkerIDFromPath(String workerPath) {
    String workerIDStr = workerPath.substring(workerPath.lastIndexOf("-") + 1);
    return Integer.parseInt(workerIDStr);
  }

  /**
   * construct job master path for the given job path
   */
  public static String constructJobMasterPath(String jobPath) {
    return jobPath + "/jm";
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
   * decode the given binary encoded WorkerInfo object
   * encoding assumed to be done by encodeWorkerZnode method
   * first 4 bytes is the length. remaining bytes are encoded WorkerInfo bytes
   */
  public static Pair<WorkerInfo, WorkerState> decodeWorkerZnode(byte[] encodedBytes) {

    if (encodedBytes == null) {
      return null;
    }

    // first 4 bytes is the length
    int state = intFromBytes(encodedBytes, 0);
    WorkerState workerState = WorkerState.forNumber(state);

    try {
      WorkerInfo workerInfo = WorkerInfo.newBuilder()
          .mergeFrom(encodedBytes, 4, encodedBytes.length - 4)
          .build();
      return new ImmutablePair<>(workerInfo, workerState);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Could not decode received byte array as a WorkerInfo object", e);
      return null;
    }
  }

  /**
   * decode the given binary encoded job master znode
   * encoding assumed to be done by encodeJobMasterZnode method
   * first 4 bytes is the length. remaining bytes are encoded masterAddress
   */
  public static Pair<String, JobMasterState> decodeJobMasterZnode(byte[] encodedBytes) {

    if (encodedBytes == null) {
      return null;
    }

    // first 4 bytes is the length
    int state = intFromBytes(encodedBytes, 0);
    JobMasterState jmState = JobMasterState.forNumber(state);
    String masterAddress =
        new String(encodedBytes, 4, encodedBytes.length - 4, StandardCharsets.UTF_8);
    return new ImmutablePair<>(masterAddress, jmState);
  }

  /**
   * encode the given WorkerInfo object as a byte array.
   * First put the worker state as a 4 byte array to the beginning
   * resulting byte array has the state bytes and workerInfo object after that
   */
  public static byte[] encodeWorkerZnode(WorkerInfo workerInfo, int state) {
    byte[] stateBytes = Ints.toByteArray(state);
    byte[] workerInfoBytes = workerInfo.toByteArray();

    return Bytes.concat(stateBytes, workerInfoBytes);
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

  /**
   * construct an int from four bytes starting at the given index
   */
  public static int intFromBytes(byte[] byteArray, int startIndex) {
    // provide 4 bytes of length int
    return Ints.fromBytes(
        byteArray[startIndex],
        byteArray[startIndex + 1],
        byteArray[startIndex + 2],
        byteArray[startIndex + 3]);
  }

}
