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
package edu.iu.dsc.tws.api.tset.sources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;

/**
 * A Hadoop source for reading the values. The source is attached to a map function.
 * Immediately after the record reader reads values, it passes them to map. This allows
 * twister2 to use Hadoop built in record readers without issue.
 *
 * @param <K> Key
 * @param <V> Value
 * @param <F> InputFormat
 * @param <I> Output
 */
public class HadoopSourceWithMap<K, V, F extends InputFormat<K, V>, I>
    implements SourceFunc<I> {
  /**
   * InputFormat class
   */
  private Class<F> inputClazz;

  /**
   * The wrapped configurations for serialization
   */
  private HadoopConfSerializeWrapper wrappedConfiguration;

  /**
   * Assigned splits for this task
   */
  private List<InputSplit> assignedSplits = new ArrayList<>();

  /**
   * The current consuming split
   */
  private int consumingSplit = 0;

  /**
   * The current record reader
   */
  private RecordReader<K, V> currentReader;

  /**
   * The initialized InputFormat
   */
  private InputFormat<K, V> format;

  /**
   * Job configuration
   */
  private JobConf jconf;

  /**
   * TSet context
   */
  private TSetContext context;

  /**
   * Map function
   */
  private MapFunc<I, Tuple<K, V>> mapFunc;

  public HadoopSourceWithMap(Configuration conf, Class<F> inputClazz,
                             MapFunc<I, Tuple<K, V>> mapFunc) {
    this.inputClazz = inputClazz;
    this.wrappedConfiguration = new HadoopConfSerializeWrapper(conf);
    this.mapFunc = mapFunc;
  }

  @Override
  public void prepare(TSetContext ctx) {
    this.context = ctx;
    Configuration hadoopConf = this.wrappedConfiguration.getConfiguration();
    jconf = new JobConf(hadoopConf);
    try {
      format = inputClazz.newInstance();
      JobContext jobContext = new JobContextImpl(hadoopConf, new JobID(context.getName(),
          context.getId()));
      List<InputSplit> splits = format.getSplits(jobContext);

      for (int i = 0; i < splits.size(); i++) {
        if (i % context.getParallelism() == context.getIndex()) {
          assignedSplits.add(splits.get(i));
        }
      }

      if (assignedSplits.size() > 0) {
        TaskID taskID = new TaskID(context.getName(), context.getId(),
            TaskType.MAP, context.getIndex());
        TaskAttemptID taskAttemptID = new TaskAttemptID(taskID, context.getId());
        TaskAttemptContextImpl taskAttemptContext =
            new TaskAttemptContextImpl(jconf, taskAttemptID);
        currentReader = format.createRecordReader(assignedSplits.get(consumingSplit),
            taskAttemptContext);
        currentReader.initialize(assignedSplits.get(consumingSplit), taskAttemptContext);
      }
    } catch (InstantiationException | IllegalAccessException
        | InterruptedException | IOException e) {
      throw new RuntimeException("Failed to initialize hadoop input", e);
    }
  }

  @Override
  public boolean hasNext() {
    if (currentReader != null) {
      try {
        boolean current = currentReader.nextKeyValue();
        while (!current && consumingSplit < assignedSplits.size() - 1) {
          TaskID taskID = new TaskID(context.getName(), context.getId(),
              TaskType.MAP, context.getIndex());
          TaskAttemptID taskAttemptID = new TaskAttemptID(taskID, context.getId());
          consumingSplit++;
          TaskAttemptContextImpl taskAttemptContext =
              new TaskAttemptContextImpl(jconf, taskAttemptID);
          currentReader = format.createRecordReader(assignedSplits.get(consumingSplit),
              taskAttemptContext);
          currentReader.initialize(assignedSplits.get(consumingSplit), taskAttemptContext);
          current = currentReader.nextKeyValue();
        }
        return current;
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Failed to read the next key vale", e);
      }
    }
    return false;
  }

  @Override
  public I next() {
    try {
      Tuple<K, V> kvTuple = new Tuple<>(currentReader.getCurrentKey(),
          currentReader.getCurrentValue());
      return mapFunc.map(kvTuple);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Failed to read the key - value", e);
    }
  }
}
