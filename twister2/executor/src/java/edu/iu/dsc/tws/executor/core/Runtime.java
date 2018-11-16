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
package edu.iu.dsc.tws.executor.core;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * Captures the runtime information about the system.
 */
public class Runtime {
  /**
   * Name of the job
   */
  private String jobName;

  /**
   * The job directory
   */
  private Path parentpath;

  /**
   * Execution plan
   */
  private ExecutionPlan plan;

  /**
   * The communication channel
   */
  private TWSChannel channel;

  public Runtime(String jName, ExecutionPlan execPlan, TWSChannel ch) {
    this.jobName = jName;
    this.plan = execPlan;
    this.channel = ch;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setJobName(Config config) {
    this.setJobName(config.getStringValue(Context.JOB_NAME));
  }

  public Path getParentpath() {
    return parentpath;
  }

  public void setParentpath(Path parentpath) {
    this.parentpath = parentpath;
  }

  public <T> DataSource<T> createInput(Config cfg, TaskContext context, InputFormat<T, ?> input) {
    return new DataSource<>(cfg, input, context.getParallelism());
  }
}
