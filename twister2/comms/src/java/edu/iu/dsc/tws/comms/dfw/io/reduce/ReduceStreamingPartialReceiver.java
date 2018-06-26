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
package edu.iu.dsc.tws.comms.dfw.io.reduce;

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.ReduceFunction;

public class ReduceStreamingPartialReceiver extends ReduceStreamingReceiver {
  private static final Logger LOG = Logger.getLogger(
      ReduceStreamingPartialReceiver.class.getName());

  public ReduceStreamingPartialReceiver(int dst, ReduceFunction function) {
    super(dst, function);
    this.reduceFunction = function;
  }

  @Override
  public boolean handleMessage(int source, Object message, int flags, int dest) {
    return this.operation.sendPartial(source, message, flags, dest);
  }
}
