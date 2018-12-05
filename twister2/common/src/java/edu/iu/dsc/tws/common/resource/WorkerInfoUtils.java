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
package edu.iu.dsc.tws.common.resource;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.NodeInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class WorkerInfoUtils {
  public static final Logger LOG = Logger.getLogger(WorkerInfoUtils.class.getName());

  private WorkerInfoUtils() { }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort) {
    return createWorkerInfo(workerID, workerIP, workerPort, null, null, null);
  }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort,
                                            NodeInfo nodeInfo) {
    return createWorkerInfo(workerID, workerIP, workerPort, nodeInfo, null, null);
  }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort,
                                            NodeInfo nodeInfo,
                                            JobAPI.ComputeResource computeResource) {
    return createWorkerInfo(workerID, workerIP, workerPort, nodeInfo, computeResource, null);
  }

  public static WorkerInfo createWorkerInfo(int workerID,
                                            String workerIP,
                                            int workerPort,
                                            NodeInfo nodeInfo,
                                            JobAPI.ComputeResource computeResource,
                                            Map<String, Integer> additionalPorts) {

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

    if (additionalPorts != null) {
      builder.putAllAdditionalPort(additionalPorts);
    }

    return builder.build();
  }

  public static WorkerInfo updateWorkerID(WorkerInfo workerInfo, int workerID) {

    return createWorkerInfo(workerID,
        workerInfo.getWorkerIP(),
        workerInfo.getPort(),
        workerInfo.getNodeInfo(),
        workerInfo.getComputeResource(),
        workerInfo.getAdditionalPortMap());
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
