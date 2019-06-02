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
import edu.iu.dsc.tws.task.api.window.function.FoldWindowedFunction;

public abstract class FoldWindow<T, K> extends BaseWindowedSink<T> {

  public abstract boolean fold(K content);

  public abstract boolean foldLateMessage(K lateMessage);

  private FoldWindowedFunction<T, K> foldWindowedFunction;

  public FoldWindow(FoldWindowedFunction<T, K> foldWindowedFunction) {
    this.foldWindowedFunction = foldWindowedFunction;
  }

  @Override
  public boolean execute(IWindowMessage<T> windowMessage) {
    if (windowMessage != null) {
      T current = null;
      K output = null;
      for (IMessage<T> msg : windowMessage.getWindow()) {
        T value = msg.getContent();
        if (current == null) {
          current = value;
        } else {
          current = foldWindowedFunction.onMessage(current, value);
          output = foldWindowedFunction.computeFold();
          fold(output);
        }
      }
    }
    return true;
  }

  @Override
  public boolean getLateMessages(IMessage<T> lateMessages) {
    T message = lateMessages.getContent();
    K outMessage = null;
    /**
     * User can create a custom output logic for late messages by extending this class
     * For example CustomFoldWindow can keep the last received late tuple and compare it
     * with the new late message and do a computation
     * */
    if (message != null) {
      T newMessage = foldWindowedFunction.onMessage(null, message);
      outMessage = foldWindowedFunction.computeFold();
      foldLateMessage(outMessage);
    }
    return true;
  }
}
