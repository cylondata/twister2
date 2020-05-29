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
package edu.iu.dsc.tws.restarter;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import static edu.iu.dsc.tws.api.config.Context.JOB_ID;

/**
 * todo bundled as a separate jar due to cyclic dependency issues
 */
public final class CheckpointedJobRestarter {

  private static final Logger LOG = Logger.getLogger(CheckpointedJobRestarter.class.getName());

  private CheckpointedJobRestarter() {

  }

  public static void main(String[] args) {
    if (args.length == 0) {
      LOG.severe("Job id is not specified.");
      return;
    }
    LOG.info("Restarting job " + args[0]);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(JOB_ID, args[0]);
    configMap.put(CheckpointingContext.CHECKPOINTING_RESTORE_JOB, true);

    Config config = ResourceAllocator.loadConfig(configMap);
    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setWorkerClass("")
        .addComputeResource(0, 0, 1)
        .setJobName("Job Restarter").build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
