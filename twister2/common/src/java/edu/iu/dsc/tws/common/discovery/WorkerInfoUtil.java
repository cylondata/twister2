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
package edu.iu.dsc.tws.common.discovery;

import java.util.List;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.NodeInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class WorkerInfoUtil {

  private WorkerInfoUtil() { }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort) {
    return createWorkerInfo(workerID, workerIP, workerPort, null, null);
  }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort,
                                            NodeInfo nodeInfo) {
    return createWorkerInfo(workerID, workerIP, workerPort, nodeInfo, null);
  }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort,
                                            NodeInfo nodeInfo,
                                            JobAPI.ComputeResource computeResource) {

    WorkerInfo.Builder builder = WorkerInfo.newBuilder();
    builder.setWorkerID(workerID);
    builder.setWorkerIP(workerIP);
    builder.setPort(workerPort);

    if (nodeInfo != null) {
      builder.setNodeInfo(nodeInfo);
    }

    if (computeResource != null) {
      builder.setComputeResource(computeResource);
    }

    return builder.build();
  }

  public static WorkerInfo updateWorkerID(WorkerInfo workerInfo, int workerID) {

    return createWorkerInfo(workerID,
        workerInfo.getWorkerIP(),
        workerInfo.getPort(),
        workerInfo.getNodeInfo(),
        workerInfo.getComputeResource());
  }

  /**
   * encode the given WorkerNetworkInfo object fields as a single line of a String
   * encoding has two parts:
   *   first part consists of ip, port and workerID separated by comma
   *   second part consists of NodeInfoUtil object
   * two parts are separated by a semicolon
   * @return
   */
  public static String encodeWorkerInfo(WorkerInfo workerInfo) {
    return workerInfo.getWorkerID() + "," + workerInfo.getWorkerIP() + "," + workerInfo.getPort()
        + ";" + NodeInfoUtil.encodeNodeInfo(workerInfo.getNodeInfo());
  }


  /**
   * decode the given String that is encoded with the method encodeWorkerNetworkInfo
   * each field is separated by a comma
   * @return
   */
  public static WorkerInfo decodeWorkerInfo(String networkInfoStr) {
    if (networkInfoStr == null) {
      return null;
    }

    String[] twoParts = networkInfoStr.split(";");
    NodeInfo nodeInfo = NodeInfoUtil.decodeNodeInfo(twoParts[1]);

    String[] fields = twoParts[0].split(",");
    if (fields.length != 3) {
      return null;
    }

    int workerID = Integer.parseInt(fields[0]);
    String workerIP = fields[1];
    int workerPort = Integer.parseInt(fields[2]);

    return createWorkerInfo(workerID, workerIP, workerPort, nodeInfo);
  }

  /**
   * convert the worker list to string for logging purposes
   * @param workers
   * @return
   */
  public static String workerListAsString(List<WorkerInfo> workers) {
    if (workers == null) {
      return null;
    }

    StringBuilder buffer = new StringBuilder();
    buffer.append("Number of workers: ").append(workers.size()).append("\n");
    int i = 0;
    for (WorkerInfo worker : workers) {
      buffer.append(worker);
    }

    return buffer.toString();
  }



}
