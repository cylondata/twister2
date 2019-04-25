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
package edu.iu.dsc.tws.task.api.window.policy;

import edu.iu.dsc.tws.task.api.IMessage;

public class WindowingTumblingPolicyManager<T> extends WindowingPolicyManager<T>
    implements IWindowingPolicyManager<T> {

  @Override
  public WindowingPolicy initialize(WindowingPolicy win) {
    windowingPolicy = win;
    return windowingPolicy;
  }

  @Override
  public boolean clearWindow() {
    windows.clear();
    return false;
  }

  @Override
  protected boolean execute(IMessage<T> message) {
    boolean status = false;
    if(message.getContent() != null) {

      status = true;
    }
    return status;
  }
}
