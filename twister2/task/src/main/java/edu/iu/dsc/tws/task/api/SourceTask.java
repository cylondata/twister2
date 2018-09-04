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
package edu.iu.dsc.tws.task.api;

import edu.iu.dsc.tws.common.config.Config;

public abstract class SourceTask implements ISource {
  private static final long serialVersionUID = -254264120110286748L;
  private TaskContext ctx;
  private Config config;


  @Override
  public void execute() {

  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    this.config = cfg;
    this.ctx = context;
  }

  public TaskContext getContext() {
    return this.ctx;
  }

}
