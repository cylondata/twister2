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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.task.window.api.GlobalStreamId;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.window.util.WindowParameter;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class WindowComputeOp<O, I> extends BaseWindowedSink<I> implements Receptor,
    Serializable {

  private TSetContext tSetContext = new TSetContext();

  // keys of the data partitions this op receives
  private IONames receivables;

  // map (TSetID --> input Name)
  private Map<String, String> rcvTSets;

  private MultiEdgeOpAdapter multiEdgeOpAdapter;

  private ComputeFunc<O, Iterator<I>> computeFunction;

  private ReduceFunc reduceFunc;

  public WindowComputeOp(ComputeFunc<O, Iterator<I>> computeFunction,
                         WindowParameter winParam) {
    this.computeFunction = computeFunction;
    this.windowParameter = winParam;
  }

  public WindowComputeOp(ComputeFunc<O, Iterator<I>> computeFunction,
                         BaseTSet originTSet, Map<String, String> receivables,
                         WindowParameter winParam) {
    this.computeFunction = computeFunction;
    this.initialize(originTSet, receivables, winParam);
  }

  public WindowComputeOp(ComputeFunc<O, Iterator<I>> computeFunction,
                         BaseTSet originTSet, Map<String, String> receivables,
                         WindowParameter winParam, ReduceFunc reduceFn) {
    this.computeFunction = computeFunction;
    this.reduceFunc = reduceFn;
    this.initialize(originTSet, receivables, winParam);
  }

  public void initialize(BaseTSet origin, Map<String, String> recvs,
                         WindowParameter winParm) {
    this.windowParameter = winParm;
    this.receivables = IONames.declare(recvs.keySet());
    this.rcvTSets = recvs;

    if (origin != null) {
      this.tSetContext.setId(origin.getId());
      this.tSetContext.setName(origin.getName());
      this.tSetContext.setParallelism(origin.getParallelism());
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.multiEdgeOpAdapter = new MultiEdgeOpAdapter(ctx);
  }

  @Override
  public boolean execute(IWindowMessage<I> windowMessage) {
    List<IMessage<I>> output = windowMessage.getWindow();
    Iterator<IMessage<I>> itrMsg = output.stream().iterator();
    Iterator<I> iterator = new Iterator<I>() {
      @Override
      public boolean hasNext() {
        return itrMsg.hasNext();
      }

      @Override
      public I next() {
        return itrMsg.next().getContent();
      }
    };

    writeToEdges(this.computeFunction.compute(iterator));
    //this.computeFunction.close();
    return true;
  }

  @Override
  public boolean getExpire(IWindowMessage<I> expiredMessages) {
    // TODO implement this
    return true;
  }

  @Override
  public boolean getLateMessages(IMessage<I> lateMessages) {
    // TODO implement this
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

  @Override
  public void add(String key, DataPartition<?> data) {
    // when it is sent to the tset context, users would not know about the key here. Therefore,
    // translate it to the user specified key
    this.tSetContext.addInput(rcvTSets.get(key), data);
  }

  @Override
  public IONames getReceivableNames() {
    return receivables;
  }

  TSetContext gettSetContext() {
    return tSetContext;
  }

  public ComputeFunc<O, Iterator<I>> getFunction() {
    return computeFunction;
  }


}
