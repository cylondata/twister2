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
package edu.iu.dsc.tws.api.task.dataparallelimpl;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.api.out.TextOutputWriter;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataSink;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataParallelSinkTaskImpl extends BaseSink {

  private static final Logger LOG = Logger.getLogger(DataParallelSourceTaskImpl.class.getName());

  private static final long serialVersionUID = -1L;

  private DataSource<String, ?> source;

  private DataSink<String> sink;

  private String datainputDirectory;
  private String centroidinputDirectory;
  private String outputDirectory;
  private int numFiles;
  private int datapointSize;
  private int centroidSize;
  private int dimension;
  private int sizeOfMargin = 100;

  @Override
  public boolean execute(IMessage message) {
    LOG.info("Message Values are::::" + message);
    return true;
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);

    datainputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_DINPUT_DIRECTORY);
    centroidinputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_CINPUT_DIRECTORY);
    outputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_OUTPUT_DIRECTORY);
    numFiles = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_NUMBER_OF_FILES));
    datapointSize = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_DSIZE));
    centroidSize = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_CSIZE));
    dimension = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_DIMENSIONS));

    ExecutionRuntime runtime = (ExecutionRuntime) config.get(
        ExecutorContext.TWISTER2_RUNTIME_OBJECT);

    boolean shared = cfg.getBooleanValue(DataParallelConstants.ARGS_SHARED_FILE_SYSTEM);
    if (!shared) {
      this.source = runtime.createInput(cfg, context,
          new LocalTextInputPartitioner(new Path(datainputDirectory), context.getParallelism()));
    } else {
      this.source = runtime.createInput(cfg, context,
          new SharedTextInputPartitioner(new Path(datainputDirectory)));
    }
    this.sink = new DataSink<>(cfg,
        new TextOutputWriter(FileSystem.WriteMode.OVERWRITE, new Path(outputDirectory)));

    LOG.info("This source is::::::::::::" + this.source + "\t" + this.sink);
  }
}
