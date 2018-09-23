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
package edu.iu.dsc.tws.examples.internal.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchCompute;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public class TaskExamples {
  private static final Logger LOG = Logger.getLogger(TaskExamples.class.getName());

  /**
   * Examples For batch and Streaming
   **/

  protected static class ReduceSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public ReduceSourceTask() {

    }

    public ReduceSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class ReduceSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Batch Reduce Message Received : " + Arrays.toString((int[]) object));
        }
      }

      return true;
    }
  }

  protected static class AllReduceSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public AllReduceSourceTask() {

    }

    public AllReduceSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class AllReduceSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      Object object = message.getContent();
      if (object instanceof int[]) {
        LOG.info("Batch AllReduce Message Received : " + Arrays.toString((int[]) object));
      }
      return true;
    }
  }

  protected static class GatherSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public GatherSourceTask() {

    }

    public GatherSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class GatherSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;

      Object object = message.getContent();
      if (object instanceof int[]) {
        LOG.info("Batch Gather Message Received : " + Arrays.toString((int[]) object));
      } else if (object instanceof Iterator) {
        Iterator<?> it = (Iterator<?>) object;
        String out = "";
        while (it.hasNext()) {
          if (it.next() instanceof int[]) {
            int[] a = (int[]) it.next();
            out += Arrays.toString(a);
          }
        }

        LOG.info("Batch Gather Message Received : " + out);
      } else {
        LOG.info("Class : " + object.getClass().getName());
      }

      return true;
    }
  }

  protected static class AllGatherSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public AllGatherSourceTask() {

    }

    public AllGatherSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class AllGatherSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;

      Object object = message.getContent();
      if (object instanceof int[]) {
        LOG.info("Batch AllGather Message Received : " + Arrays.toString((int[]) object));
      } else if (object instanceof Iterator) {
        Iterator<?> it = (Iterator<?>) object;
        String out = "";
        while (it.hasNext()) {
          if (it.next() instanceof int[]) {
            int[] a = (int[]) it.next();
            out += Arrays.toString(a);
          }
        }
        LOG.info("Batch AllGather Message Received : " + out);
      } else {
        LOG.info("Class : " + object.getClass().getName());
      }

      return true;
    }
  }

  protected static class PartitionSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public PartitionSourceTask() {

    }

    public PartitionSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class PartitionSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {

      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          ((Iterator) message.getContent()).next();
          count++;
        }
        if (count % 1 == 0) {
          System.out.println("Message Partition Received : " + message.getContent()
              + ", Count : " + count);
        }
      }

      return true;
    }
  }

  protected static class KeyedReduceSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public KeyedReduceSourceTask() {

    }

    public KeyedReduceSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 1000) {
        if (context.writeEnd(edge, "" + count, val)) {
          count++;
        }
      } else if (count < 1000) {
        if (context.write(edge, "" + count, val)) {
          count++;
        }
      }
    }
  }

  protected static class KeyedReduceSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      Object object = message.getContent();
      if (object instanceof KeyedContent) {
        KeyedContent keyedContent = (KeyedContent) object;
        if (keyedContent.getValue() instanceof int[]) {
          int[] a = (int[]) keyedContent.getValue();
          LOG.info("Message Keyed-Reduced : " + keyedContent.getKey() + ", "
              + Arrays.toString(a));
        }
      }
      count++;

      return true;
    }
  }

  protected static class KeyedGatherSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public KeyedGatherSourceTask() {

    }

    public KeyedGatherSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 1000) {
        if (context.writeEnd(edge, "" + count, val)) {
          count++;
        }
      } else if (count < 1000) {
        if (context.write(edge, "" + count, val)) {
          count++;
        }
      }
    }
  }

  protected static class KeyedGatherSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.info("Message Keyed-Gather : " + message.getContent()
          + ", Count : " + count);
      count++;

      return true;
    }
  }

  protected static class BroadcastSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public BroadcastSourceTask() {

    }

    public BroadcastSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      context.write(edge, "Hello");
    }
  }

  protected static class BroadcastSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      System.out.println(" Message Braodcasted : "
          + message.getContent() + ", counter : " + count);
      count++;

      return true;
    }
  }

  /**
   * Iterative Job Example
   **/

  protected static class IterativeSourceTask extends BaseBatchSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataSet<Object> input;

    private int count = 0;

    private String edge;

    public IterativeSourceTask() {

    }

    public IterativeSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd(edge, "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(edge, "Hello")) {
          count++;
        }
      }
    }

    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      input = data;
    }
  }

  protected static class IterativeSinkTask extends BaseBatchSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    private List<String> list = new ArrayList<>();

    private int count;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received message: " + message.getContent());

      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          count++;
          list.add(ret.toString());
        }
        LOG.info("Message Partition Received : " + message.getContent()
            + ", Count : " + count);
      }
      count++;
      return true;
    }

    @Override
    public Partition<Object> get() {
      return new Partition<>(context.taskIndex(), list);
    }
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////

  /***
   * Streaming Task Examples
   * **/

  protected static class SReduceSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count;
    private String edge;

    public SReduceSourceTask() {

    }

    public SReduceSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Reduce sent count : %d", context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SReduceSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 100 == 0) {
        LOG.info("Streaming Reduce receive count : " + count);
      }
      count++;
      return true;
    }
  }

  protected static class SAllReduceSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SAllReduceSourceTask() {
    }

    public SAllReduceSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "Hello");
      if (wrote) {
        count++;
        if (count % 1000 == 0) {
          LOG.info(String.format("%d %d AllReduce sent count : %d", context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SAllReduceSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 1000 == 0) {
        LOG.info("Message AllReduced : " + message.getContent() + ", Count : " + count);
      }
      count++;
      return true;
    }
  }

  protected static class SGatherSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private String edge;

    private int count = 0;

    public SGatherSourceTask() {

    }

    public SGatherSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {

      // lets generate a message
      int[] a = {1};
      boolean wrote = context.write(edge, a);
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Reduce sent count : %d", context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SGatherSinkTask extends BaseStreamSink {
    private int count = 0;
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public boolean execute(IMessage message) {
      if (count % 100 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Stream Message Gathered : " + Arrays.toString((int[]) object)
              + ", Count : " + count);
        } else if (object instanceof ArrayList) {
          ArrayList<?> a = (ArrayList<?>) object;
          String out = "";
          for (int i = 0; i < a.size(); i++) {
            Object o = a.get(i);
            if (o instanceof int[]) {
              out += Arrays.toString((int[]) o);
            }
          }
          LOG.info("Stream Message Gathered : " + out + ", Count : " + count);
        } else {
          LOG.info("Stream Message Gathered : " + message.getContent().getClass().getName()
              + ", Count : " + count);
        }

      }
      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
      }
      return true;
    }
  }

  protected static class SAllGatherSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SAllGatherSourceTask() {

    }

    public SAllGatherSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      int[] a = {1};
      boolean wrote = context.write(edge, a);
      if (wrote) {
        count++;
        if (count % 10000 == 0) {

          LOG.info(String.format("%d %d Message Gather sent count : %d", context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SAllGatherSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 1000 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Stream Message AllGathered : " + Arrays.toString((int[]) object)
              + ", Count : " + count);
        } else if (object instanceof ArrayList) {
          ArrayList<?> a = (ArrayList<?>) object;
          String out = "";
          for (int i = 0; i < a.size(); i++) {
            Object o = a.get(i);
            if (o instanceof int[]) {
              out += Arrays.toString((int[]) o);
            }
          }
          LOG.info("Stream Message AllGathered : " + out + ", Count : " + count);
        } else if (object instanceof Iterator) {
          Iterator<?> it = (Iterator<?>) object;
          String out = "";
          while (it.hasNext()) {
            if (it.next() instanceof int[]) {
              int[] a = (int[]) it.next();
              out += Arrays.toString(a);
            }
          }
          LOG.info("Stream AllGather Message Received : " + out);
        }
      }
      count++;
      return true;
    }
  }

  protected static class SBroadCastSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SBroadCastSourceTask() {
    }

    public SBroadCastSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "Hello");
      if (wrote) {
        count++;
        if (count % 1000 == 0) {
          LOG.info(String.format("%d %d Streaming Message Broadcast sent count : %d",
              context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SBroadCastSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private static int counter = 0;

    @Override
    public boolean execute(IMessage message) {
      if (counter % 1000 == 0) {
        System.out.println(context.taskId() + " Message Braodcasted : "
            + message.getContent() + ", counter : " + counter);
      }
      counter++;
      return true;
    }
  }


  protected static class SKeyedPartitionSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    private String edge;

    public SKeyedPartitionSourceTask() {
    }

    public SKeyedPartitionSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "key", "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Streaming Message Partition sent count : %d",
              context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SKeyedPartitionSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
      }
      LOG.info(String.format("%d %d Streaming Message Partition Received count: %d",
          context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }

  protected static class SKeyedReduceSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private String edge;

    private int count;

    public SKeyedReduceSourceTask() {
    }

    public SKeyedReduceSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "" + count, "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Streaming Reduce sent count : %d", context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SKeyedReduceSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 100 == 0) {
        System.out.println("Streaming Message Keyed-Reduced : " + message.getContent()
            + ", Count : " + count);
      }
      count++;
      return true;
    }
  }

  protected static class SPartitionSourceTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    private String edge;

    public SPartitionSourceTask() {
    }

    public SPartitionSourceTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      boolean wrote = context.write(edge, "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Streaming Message Partition sent count : %d",
              context.getWorkerId(),
              context.taskId(), count));
        }
      }
    }
  }

  protected static class SPartitionSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
      }
      LOG.info(String.format("%d %d Streaming Message Partition Received count: %d",
          context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }


  public BaseBatchSource getBatchSourceClass(String example, String edge) {
    BaseBatchSource source = null;
    if ("reduce".equals(example)) {
      source = new ReduceSourceTask(edge);
    }
    if ("allreduce".equals(example)) {
      source = new AllReduceSourceTask(edge);
    }
    if ("gather".equals(example)) {
      source = new GatherSourceTask(edge);
    }
    if ("allgather".equals(example)) {
      source = new AllGatherSourceTask(edge);
    }
    if ("partition".equals(example)) {
      source = new PartitionSourceTask(edge);
    }
    if ("keyed-reduce".equals(example)) {
      source = new KeyedReduceSourceTask(edge);
    }
    if ("keyed-gather".equals(example)) {
      source = new KeyedGatherSourceTask(edge);
    }
    if ("bcast".equals(example)) {
      source = new BroadcastSourceTask(edge);
    }
    if ("iterative-source".equals(example)) {
      source = new IterativeSourceTask(edge);
    }
    return source;
  }

  public BaseBatchSink getBatchSinkClass(String example) {
    BaseBatchSink sink = null;
    if ("reduce".equals(example)) {
      sink = new ReduceSinkTask();
    }
    if ("allreduce".equals(example)) {
      sink = new AllReduceSinkTask();
    }
    if ("gather".equals(example)) {
      sink = new GatherSinkTask();
    }
    if ("allgather".equals(example)) {
      sink = new AllGatherSinkTask();
    }
    if ("partition".equals(example)) {
      sink = new PartitionSinkTask();
    }
    if ("keyed-reduce".equals(example)) {
      sink = new KeyedReduceSinkTask();
    }
    if ("keyed-gather".equals(example)) {
      sink = new KeyedGatherSinkTask();
    }
    if ("bcast".equals(example)) {
      sink = new BroadcastSinkTask();
    }
    if ("iterative-sink".equals(example)) {
      sink = new IterativeSinkTask();
    }
    return sink;
  }

  public BaseStreamSource getStreamSourceClass(String example, String edge) {
    BaseStreamSource source = null;
    if ("sreduce".equals(example)) {
      source = new SReduceSourceTask(edge);
    }
    if ("allreduce".equals(example)) {
      source = new SAllReduceSourceTask(edge);
    }
    if ("gather".equals(example)) {
      source = new SGatherSourceTask(edge);
    }
    if ("allgather".equals(example)) {
      source = new SAllGatherSourceTask(edge);
    }
    if ("partition".equals(example)) {
      source = new SPartitionSourceTask(edge);
    }
    if ("keyed-reduce".equals(example)) {
      source = new SKeyedReduceSourceTask(edge);
    }
    /*if ("keyed-gather".equals(example)) {
      source = new SKeyedGatherSourceTask(edge);
    }
    if ("bcast".equals(example)) {
      source = new SBroadcastSourceTask(edge);
    }*/
    return source;
  }

  public BaseStreamSink getStreamSinkClass(String example) {
    BaseStreamSink sink = null;
    if ("sreduce".equals(example)) {
      sink = new SReduceSinkTask();
    }
    if ("allreduce".equals(example)) {
      sink = new SAllReduceSinkTask();
    }
    if ("gather".equals(example)) {
      sink = new SGatherSinkTask();
    }
    if ("allgather".equals(example)) {
      sink = new SAllGatherSinkTask();
    }
    if ("partition".equals(example)) {
      sink = new SPartitionSinkTask();
    }
    if ("keyed-reduce".equals(example)) {
      sink = new SKeyedReduceSinkTask();
    }
    /*if ("keyed-gather".equals(example)) {
      sink = new SKeyedGatherSinkTask();
    }
    if ("bcast".equals(example)) {
      sink = new SBroadcastSinkTask();
    }*/
    return sink;
  }

  public BaseBatchCompute getComputeClass(String example, String edge) {
    BaseBatchCompute compute = null;

    return compute;
  }
}
