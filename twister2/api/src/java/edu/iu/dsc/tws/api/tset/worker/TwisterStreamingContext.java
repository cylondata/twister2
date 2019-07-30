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
package edu.iu.dsc.tws.api.tset.worker;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

/**
 * This is now deprecated. Use {@link edu.iu.dsc.tws.api.tset.env.StreamingTSetEnvironment} instead!
 *
 * @deprecated deprecated abstract class
 */
@Deprecated
public class TwisterStreamingContext {

  private Config config;

  private TaskExecutor taskExecutor;

  private final OperationMode mode = OperationMode.STREAMING;

  public TwisterStreamingContext(Config config, TaskExecutor taskExecutor, OperationMode mode) {
    this.config = config;
    this.taskExecutor = taskExecutor;
  }

  public TwisterStreamingContext(Config config, TaskExecutor taskExecutor) {
    this.config = config;
    this.taskExecutor = taskExecutor;
  }

  public Config getConfig() {
    return config;
  }

  public OperationMode getMode() {
    return mode;
  }

  /**
   * Create source task with the given source, The mode is taken as the mode set
   * in {@link TwisterStreamingContext#mode}
   *
   * @param source source function to be used
   * @return SourceTset created
   */
/*  public <T> StreamingSourceTSet<T> createSource(Source<T> source, int parallelism) {
    //TODO: how to make sure user sets the correct mode? before using create source, pass in mode
    TSetEnvironment tSetEnv = new TSetEnvironment(this.config, this.taskExecutor, this.mode);
    return tSetEnv.createStreamingSource(source, parallelism);
  }*/
}
