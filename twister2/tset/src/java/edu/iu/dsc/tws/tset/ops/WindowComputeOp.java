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
package edu.iu.dsc.tws.tset.ops;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.task.window.api.GlobalStreamId;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.window.util.WindowParameter;

public abstract class WindowComputeOp<O,I> extends BaseWindowedSink<I> {

  private MultiEdgeOpAdapter multiEdgeOpAdapter;
  private ComputeFunc<O, I> computeFunction;

  public WindowComputeOp(ComputeFunc<O, I> computeFunction) {
    this.computeFunction = computeFunction;
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingCountWindow(2);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.multiEdgeOpAdapter = new MultiEdgeOpAdapter(ctx);
  }

  @Override
  public boolean execute(IWindowMessage<I> windowMessage) {
    O output = (O) windowMessage.getWindow();
    //O output = computeFunction.compute(windowMessage.getContent());
    writeToEdges(output);
    writeEndToEdges();
    computeFunction.close();
    return true;
  }

  @Override
  public boolean getExpire(IWindowMessage<I> expiredMessages) {
    return true;
  }

  @Override
  public boolean getLateMessages(IMessage<I> lateMessages) {
    return true;
  }

  @Override
  public boolean execute(IMessage<I> content) {
    if (this.isTimestamped()) {
      long time = this.iTimestampExtractor.extractTimestamp(content.getContent());
      GlobalStreamId streamId = new GlobalStreamId(content.edge());
      if (this.watermarkEventGenerator.track(streamId, time)) {
        this.windowManager.add(content, time);
      } else {
        // TODO : handle the late tuple stream a delayed message won't be handled unless a
        //  late stream
        // TODO : Here another latemessage function can be called or we can bundle the late messages
        //  with the next windowing message
        this.getLateMessages(content);
      }
    } else {
      this.windowManager.add(content);
    }
    return true;

  }

  @Override
  protected WindowLifeCycleListener<I> newWindowLifeCycleListener() {
    return new WindowLifeCycleListener<I>() {
      @Override
      public void onExpiry(IWindowMessage<I> events) {
        // TODO : design the logic
        getExpire(events);
      }

      @Override
      public void onActivation(IWindowMessage<I> events, IWindowMessage<I> newEvents,
                               IWindowMessage<I> expired) {
        collectiveEvents = events;
        execute(collectiveEvents);
      }
    };
  }

  <T> void writeToEdges(T output) {
    multiEdgeOpAdapter.writeToEdges(output);
  }

  void writeEndToEdges() {
    multiEdgeOpAdapter.writeEndToEdges();
  }

  <K, V> void keyedWriteToEdges(K key, V val) {
    multiEdgeOpAdapter.keyedWriteToEdges(key, val);
  }

}
