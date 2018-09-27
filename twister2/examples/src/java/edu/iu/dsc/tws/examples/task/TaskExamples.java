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
package edu.iu.dsc.tws.examples.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;


public class TaskExamples {
  private static final Logger LOG = Logger.getLogger(TaskExamples.class.getName());

  /**
   * Examples For batch and Streaming
   **/


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


  protected static class GatherSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public boolean execute(IMessage message) {
      Object object = message.getContent();
      if (object instanceof int[]) {
        LOG.info("Batch Gather Message Received : " + Arrays.toString((int[]) object));
      } else if (object instanceof Iterator) {
        Iterator<?> it = (Iterator<?>) object;
        int[] a = {};
        ArrayList<int[]> data = new ArrayList<>();
        while (it.hasNext()) {
          if (it.next() instanceof int[]) {
            a = (int[]) it.next();
            LOG.info("Data : " + Arrays.toString(a));
            data.add(a);
          }
        }

        LOG.info("Batch Gather Task Message Receieved : " + data.size());

      } else {
        LOG.info("Class : " + object.getClass().getName());
      }
      return true;
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


  protected static class BroadcastSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      System.out.println(" Message Broadcasted : "
          + message.getContent() + ", counter : " + count);
      count++;

      return true;
    }
  }

  protected static class SReduceSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Stream Reduce Message Received : " + Arrays.toString((int[]) object));
        }
      }
      return true;
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


  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class SPartitionSinkTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          it.next();
          count += 1;
        }
      }
      LOG.info(String.format("%d %d Streaming Message Partition Received count: %d",
          context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }


  public BaseBatchSink getBatchSinkClass(String example) {
    BaseBatchSink sink = null;
    if ("reduce".equals(example)) {
      sink = new ReduceSinkTask();
    }
    if ("allreduce".equals(example)) {
      sink = new ReduceSinkTask();
    }
    if ("gather".equals(example)) {
      sink = new GatherSinkTask();
    }
    if ("allgather".equals(example)) {
      sink = new GatherSinkTask();
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
    return sink;
  }


  public BaseStreamSink getStreamSinkClass(String example) {
    BaseStreamSink sink = null;
    if ("sreduce".equals(example)) {
      sink = new SReduceSinkTask();
    }
    if ("allreduce".equals(example)) {
      sink = new SReduceSinkTask();
    }
    if ("gather".equals(example)) {
      sink = new SGatherSinkTask();
    }
    if ("allgather".equals(example)) {
      sink = new SGatherSinkTask();
    }
    if ("bcast".equals(example)) {
      sink = new SBroadCastSinkTask();
    }
    if ("partition".equals(example)) {
      sink = new SPartitionSinkTask();
    }
    if ("keyed-reduce".equals(example)) {
      sink = new SKeyedReduceSinkTask();
    }
    return sink;
  }

}
