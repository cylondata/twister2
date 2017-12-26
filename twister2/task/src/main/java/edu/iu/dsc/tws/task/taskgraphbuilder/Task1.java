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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.logging.Logger;

public class Task1 extends TaskMapper {

  private static final Logger LOGGER = Logger.getLogger(Task1.class.getName());

  private String id;

  public Task1(String id, int priorityValue) {
    super(id, priorityValue);
    this.id = id;
  }

  @Override
  public void execute() {
    for (int i = 0; i < 10; i++) {
      LOGGER.info("Task execution" + "\t" + id);
    }
  }

  @Override
  public void execute(TaskMapper taskMapper) {
    for (int i = 0; i < 10; i++) {
      LOGGER.info("Task execution" + "\t" + id);
    }
  }

  @Override
  public void execute(String message) {
    for (int i = 0; i < 10; i++) {
      LOGGER.info("Task execution" + "\t" + id + "\t" + message);
    }
  }
}

