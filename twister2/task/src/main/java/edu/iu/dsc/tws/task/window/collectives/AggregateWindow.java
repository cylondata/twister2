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
package edu.iu.dsc.tws.task.window.collectives;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.window.function.AggregateWindowedFunction;

public abstract class AggregateWindow<T> extends BaseWindowedSink<T> {

  public abstract boolean aggregate(T message);

  public abstract boolean aggregateLateMessages(T message);



  private AggregateWindowedFunction<T> aggregateWindowedFunction;

  public AggregateWindow(AggregateWindowedFunction aggregateWindowedFunction) {
    this.aggregateWindowedFunction = aggregateWindowedFunction;
  }

  @Override
  public boolean execute(IWindowMessage<T> windowMessage) {
    if (windowMessage != null) {
      T current = null;
      for (IMessage<T> msg : windowMessage.getWindow()) {
        T value = msg.getContent();
        if (current == null) {
          current = value;
        } else {
          current = aggregateWindowedFunction.onMessage(current, value);
        }
      }
      aggregate(current);
    }
    return true;
  }

  @Override
  public boolean getLateMessages(IMessage<T> lateMessages) {

    return aggregateLateMessages(lateMessages.getContent());
  }

  @Override
  public boolean getExpire(IWindowMessage<T> expiredMessages) {
    return false;
  }
}
