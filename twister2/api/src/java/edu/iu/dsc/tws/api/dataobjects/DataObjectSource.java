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
package edu.iu.dsc.tws.api.dataobjects;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataObjectSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataObjectSource.class.getName());

  private static final long serialVersionUID = -1L;

  private DataSource<String, ?> source;

  @Override
  public void execute() {
    LOG.info("Context Task Index:" + context.taskIndex());
    InputSplit<String> inputSplit = source.getNextSplit(context.taskIndex());
    int splitCount = 0;
    int totalCount = 0;
    while (inputSplit != null) {
      try {
        int count = 0;
        while (!inputSplit.reachedEnd()) {
          String value = inputSplit.nextRecord(null);
          if (value != null) {                     //Bug here...
            //LOG.info("We read value: " + value); //First index reads more value than for
            //example 100, task index 0 reads 51 & 1 reads 49
            context.write("direct", value);
            count += 1;
            totalCount += 1;
          }
        }
        splitCount += 1;
        inputSplit = source.getNextSplit(context.taskIndex());
        LOG.info("Task index:" + context.taskIndex() + " count: " + count
            + "split: " + splitCount + " total count: " + totalCount);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to read the input", e);
      }
    }
    context.end("direct");
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);

    String datainputDirectory = cfg.getStringValue(DataObjectConstants.ARGS_DINPUT_DIRECTORY);
    ExecutionRuntime runtime = (ExecutionRuntime)
        cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);

    boolean shared = cfg.getBooleanValue(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM);
    if (!shared) {
      this.source = runtime.createInput(cfg, context,
          new LocalTextInputPartitioner(new Path(datainputDirectory), context.getParallelism()));
    } else {
      this.source = runtime.createInput(cfg, context,
          new SharedTextInputPartitioner(new Path(datainputDirectory)));
    }
  }
}
