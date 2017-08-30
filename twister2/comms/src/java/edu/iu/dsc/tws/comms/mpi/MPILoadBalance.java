//  Copyright 2017 Twitter. All rights reserved.
//
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
package edu.iu.dsc.tws.comms.mpi;

import java.util.List;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageBuilder;
import edu.iu.dsc.tws.comms.api.MessageFormatter;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.InstancePlan;

public class MPILoadBalance implements DataFlowOperation {
  @Override
  public void init(Config config, InstancePlan instancePlan, List<Integer> sources,
                   List<Integer> destinations, int stream, MessageReceiver receiver,
                   MessageFormatter messageFormatter, MessageBuilder messageBuilder) {

  }

  @Override
  public void sendPartial(Message message) {

  }

  @Override
  public void finish() {

  }

  @Override
  public void sendComplete(Message message) {

  }
}
