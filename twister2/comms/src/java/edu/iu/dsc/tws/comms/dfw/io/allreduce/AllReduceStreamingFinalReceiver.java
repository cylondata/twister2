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
package edu.iu.dsc.tws.comms.dfw.io.allreduce;

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingReceiver;

public class AllReduceStreamingFinalReceiver extends ReduceStreamingReceiver {
  private static final Logger LOG = Logger.getLogger(
      AllReduceStreamingFinalReceiver.class.getName());

  private DataFlowBroadcast bcast;
  private int task;
  private int count = 0;

  public AllReduceStreamingFinalReceiver(ReduceFunction reduceFunction,
                                         DataFlowBroadcast bcast, int t) {
    super(reduceFunction);
    this.bcast = bcast;
    this.task = t;
  }

  @Override
  public boolean handleMessage(int source, Object message, int flags, int dest) {
    return bcast.send(source, message, 0);
  }
}
