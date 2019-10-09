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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.compute.nodes.ISink;
import edu.iu.dsc.tws.api.config.Config;

public class CheckpointingSGatherSink implements ISink, ICompute<Iterator<Tuple<Integer, Long>>> {

  private static final Logger LOG = Logger.getLogger(CheckpointingSGatherSink.class.getName());

  @Override
  public void prepare(Config cfg, TaskContext context) {
    LOG.info("Preparing : " + context.taskName());
  }

  @Override
  public boolean execute(IMessage<Iterator<Tuple<Integer, Long>>> content) {
    Iterator<Tuple<Integer, Long>> gatherMessage = content.getContent();
    LOG.info("Received gather message");
    while (gatherMessage.hasNext()) {
      Tuple<Integer, Long> next = gatherMessage.next();
      LOG.info(next.getKey() + "," + next.getValue());
    }
    return true;
  }
}
