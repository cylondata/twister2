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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * Twister context provides the user with the basic functionality that is needed to
 * start a Twister2 application
 */
public class TwisterContext {

  private Config config;

  private TaskExecutor taskExecutor;

  private OperationMode mode;

  public TwisterContext(Config config, TaskExecutor taskExecutor, OperationMode mode) {
    this.config = config;
    this.taskExecutor = taskExecutor;
    this.mode = mode;
  }

  public TwisterContext(Config config, TaskExecutor taskExecutor) {
    this.config = config;
    this.taskExecutor = taskExecutor;
    this.mode = OperationMode.STREAMING;
  }

  public Config getConfig() {
    return config;
  }

  public OperationMode getMode() {
    return mode;
  }

  public void setMode(OperationMode mode) {
    this.mode = mode;
  }

  /**
   * Create source task with the given source and given mode
   *
   * @param source source function to be used
   * @param opMode The Mode of the operation, Streaming or Batch
   * @return SourceTset created
   */
  public <T> TSet<T> createSource(Source<T> source, OperationMode opMode) {
    //TODO: how to make sure user sets the correct mode? before using create source, pass in mode
    TSetEnv tSetEnv = new TSetEnv(this.config, this.taskExecutor, opMode);
    return tSetEnv.createSource(source);
  }

  /**
   * Create source task with the given source, The mode is taken as the mode set
   * in {@link TwisterContext#mode}
   *
   * @param source source function to be used
   * @return SourceTset created
   */
  public <T> TSet<T> createSource(Source<T> source) {
    //TODO: how to make sure user sets the correct mode? before using create source, pass in mode
    TSetEnv tSetEnv = new TSetEnv(this.config, this.taskExecutor, this.mode);
    return tSetEnv.createSource(source);
  }
}
