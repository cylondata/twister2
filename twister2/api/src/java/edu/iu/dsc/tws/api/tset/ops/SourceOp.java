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
package edu.iu.dsc.tws.api.tset.ops;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.tset.CacheableImpl;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceOp<T> implements ISource, Receptor {
  private static final Logger LOG = Logger.getLogger(SourceOp.class.getName());

  private static final long serialVersionUID = -2400242961L;

  private TaskContext context;

  private Source<T> dataSet;

  public SourceOp() {

  }

  public SourceOp(Source<T> src) {
    this.dataSet = src;
  }

  @Override
  public void execute() {
    if (dataSet.hasNext()) {
      T t = dataSet.next();
      if (t != null) {
        context.write(Constants.DEFAULT_EDGE, t);
      }
    } else {
      context.end(Constants.DEFAULT_EDGE);
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    TSetContext tSetContext = new TSetContext(cfg, ctx.taskIndex(), ctx.globalTaskId(),
        ctx.taskName(), ctx.getParallelism(), ctx.getWorkerId(), ctx.getConfigurations());
    dataSet.prepare(tSetContext);
  }

  @Override
  public void add(String name, DataObject<?> data) {
    dataSet.addInput(name, new CacheableImpl<>(data));
  }
}
