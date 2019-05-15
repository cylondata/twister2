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

import edu.iu.dsc.tws.comms.api.ReduceFunction;

public abstract class BaseReduceBatchFinalReceiver extends ReduceBatchReceiver {
  public BaseReduceBatchFinalReceiver(ReduceFunction reduce) {
    super(reduce);
    this.reduceFunction = reduce;
  }

  @Override
  public boolean handleMessage(int source, Object message, int flags, int dest) {
    return handleFinished(source, message);
  }

  /**
   * Handle finish
   * @param task task
   * @param value value
   * @return true if success
   */
  protected abstract boolean handleFinished(int task, Object value);
}
