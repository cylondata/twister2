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
package edu.iu.dsc.tws.task.api;

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;

/**
 * The abstract class that represents the start of a application
 * This task reads data from a input data source and ouputs data to another task
 */
public abstract class SourceTask extends Task {
  /**
   * The task id's of tasks that this sources task will send messages to
   */
  private Set<Integer> sinks = new HashSet<>(); //TODO: we only need this if the dfop is created
  //TODO: at the task level will leave this for now might need it later

  /**
   * The data flow operation related to this task. This will be used to send data to the dependent
   * tasks in the application
   */
  private DataFlowOperation dfop;

  public SourceTask() {
    super();
  }

  public SourceTask(int tid) {
    super(tid);
  }

  public SourceTask(int tid, Set<Integer> sinksSet) {
    super(tid);
    this.sinks = sinksSet;
  }

  public SourceTask(int tid, DataFlowOperation dataFlowOperation) {
    super(tid);
    this.dfop = dataFlowOperation;
  }

  public Set<Integer> getSinks() {
    return sinks;
  }

  public void setSinks(Set<Integer> sinks) {
    this.sinks = sinks;
  }

  public DataFlowOperation getDfop() {
    return dfop;
  }

  public void setDfop(DataFlowOperation dfop) {
    this.dfop = dfop;
  }
}
