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
package edu.iu.dsc.tws.task.api.window.collectives;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.api.window.function.ProcessWindowedFunction;

public abstract class ProcessWindow<T> extends BaseWindowedSink<T> {
  public abstract boolean process(IWindowMessage<T> windowMessage);

  public abstract boolean processLateMessages(IMessage<T> lateMessage);

  private ProcessWindowedFunction<T> processWindowedFunction;

  public ProcessWindow(ProcessWindowedFunction<T> processWindowedFunction) {
    this.processWindowedFunction = processWindowedFunction;
  }

  @Override
  public boolean execute(IWindowMessage<T> windowMessage) {
    IWindowMessage<T> newMessage = this.processWindowedFunction.process(windowMessage);
    process(newMessage);
    return true;
  }

  @Override
  public boolean getLateMessages(IMessage<T> lateMessages) {
    return processLateMessages(this.processWindowedFunction.processLateMessage(lateMessages));
  }
}
