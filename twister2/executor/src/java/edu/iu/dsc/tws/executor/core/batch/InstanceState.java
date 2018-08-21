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
package edu.iu.dsc.tws.executor.core.batch;

public class InstanceState {
  // we start with init state
  public static final int INIT = 0;
  // execution is progressing, that means input is still active
  public static final int EXECUTING = 1 << 1;
  // execution is finished
  public static final int EXECUTION_DONE = 1 << 2;
  // started sending
  public static final int SENDING_DONE = 1 << 4;
  // output is sent
  public static final int OUT_COMPLETE = 1 << 5;
  // finish value
  public static final int FINISH =
      EXECUTING | EXECUTION_DONE | SENDING_DONE | OUT_COMPLETE;

  private int value;

  InstanceState(int val) {
    this.value = val;
  }

  public void set(int state) {
    this.value = this.value | state;
  }

  public boolean isSet(int state) {
    return (value & state) == state;
  }

  public boolean isEqual(int state) {
    return value == state;
  }

  public boolean isNotSet(int state) {
    return (value & state) != state;
  }
}
