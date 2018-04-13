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
package edu.iu.dsc.tws.executor.comm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.task.api.Message;

public class PartitionMessageReceiver implements MessageReceiver {
  private Map<Integer, BlockingQueue<Message>> inMessages;

  private Config taskConfig;

  public PartitionMessageReceiver(Config tConfig, Map<Integer, BlockingQueue<Message>> inMessages) {
    this.inMessages = inMessages;
    this.taskConfig = tConfig;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    return false;
  }

  @Override
  public void progress() {

  }
}
