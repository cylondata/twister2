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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceOp<T> implements ISource {
  private TaskContext context;

  private DataSet<T> dataSet;

  public SourceOp(DataSource<T, ?> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public void execute() {
    context.write("", null);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
  }
}
