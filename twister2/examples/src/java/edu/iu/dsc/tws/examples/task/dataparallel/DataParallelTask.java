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
package edu.iu.dsc.tws.examples.task.dataparallel;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.examples.comms.Constants;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;

public class DataParallelTask extends BaseBatchSource {
  private static final Logger LOG = Logger.getLogger(DataParallelTask.class.getName());

  private DataSource<String> source;

  @Override
  public void execute() {
    InputSplit<String> inputSplit = source.getSplit(context.taskIndex());
    try {
      while (!inputSplit.reachedEnd()) {
        String value = inputSplit.nextRecord(null);
        LOG.info("We read value: " + value);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read the input", e);
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);

    String directory = cfg.getStringValue(Constants.ARGS_INPUT_DIRECTORY);
    ExecutionRuntime runtime = (ExecutionRuntime) config.get(
        ExecutorContext.TWISTER2_RUNTIME_OBJECT);

    this.source = runtime.createInput(cfg, context,
        new LocalTextInputPartitioner(new Path(directory)));
  }
}
