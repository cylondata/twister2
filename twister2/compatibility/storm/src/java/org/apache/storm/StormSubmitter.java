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

package org.apache.storm;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.twister2.Twister2StormWorker;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

/**
 * Use this class to submit storm topologies to run on the twister2 cluster.
 */
public final class StormSubmitter {

  private StormSubmitter() {
  }

  /**
   * Submits a topology to run on the cluster. A topology runs forever or until
   * explicitly killed.
   *
   * @param name the name of the storm.
   * @param stormConfig the topology-specific configuration. See {@link Config}.
   * @param topology the processing to execute.
   */
  @SuppressWarnings("rawtypes")
  public static void submitTopology(
      String name,
      Map stormConfig,
      StormTopology topology) {
    edu.iu.dsc.tws.common.config.Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(stormConfig);
    Gson gson = new Gson();
    String tg = gson.toJson(topology.getT2DataFlowTaskGraph());
    jobConfig.put("storm-topology", topology.getT2DataFlowTaskGraph());

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(name);
    jobBuilder.setWorkerClass(Twister2StormWorker.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
