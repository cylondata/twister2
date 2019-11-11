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
package edu.iu.dsc.tws.checkpointing.task;

import java.util.Iterator;
import java.util.Optional;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.config.Config;

public class CheckpointingSGatherSink implements ICompute<Iterator<Tuple<Integer, Long>>> {

  private static final Logger LOG = Logger.getLogger(CheckpointingSGatherSink.class.getName());

  public static final String FT_GATHER_EDGE = "ft-gather-edge";
  private TaskContext taskContext;
  private String parentTaskName;

  private int parentTaskId;

  public CheckpointingSGatherSink(String parentTaskName) {
    this.parentTaskName = parentTaskName;
  }

  public int getParentTaskId() {
    return parentTaskId;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    this.taskContext = context;
    Optional<TaskInstancePlan> first = context.getTasksByName(this.parentTaskName)
        .stream().findFirst();
    first.ifPresent(taskInstancePlan -> this.parentTaskId = taskInstancePlan.getTaskId());
  }

  @Override
  public boolean execute(IMessage<Iterator<Tuple<Integer, Long>>> content) {
    // no need to do anything
    return true;
  }
}
