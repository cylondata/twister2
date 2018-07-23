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
package edu.iu.dsc.tws.executor.comm.tasks.streaming;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.core.SourceTaskContextListener;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceStreamTask implements ISource {

  private static final long serialVersionUID = -254264120110286748L;
  private TaskContext ctx;
  private Config config;
  private SourceTaskContextListener sourceTaskContextListener;

  @Override
  public void run() {

  }

  @Override
  public void interrupt() {

  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    this.config = cfg;
    this.ctx = context;
  }

  public TaskContext getContext() {
    return ctx;
  }

  public void setContext(TaskContext context) {
    this.ctx = context;
  }

  public SourceTaskContextListener getSourceTaskContextListener() {
    return sourceTaskContextListener;
  }

  public void setSourceTaskContextListener(SourceTaskContextListener sourceTaskContextListener) {
    this.sourceTaskContextListener = sourceTaskContextListener;
  }
}
