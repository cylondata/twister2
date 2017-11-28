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
package edu.iu.dsc.tws.task.tsystem;

public abstract class AbstractMapper implements Runnable {

  private final String id;
  private int priorityValue;
  private TaskGraphParser taskGraphParser;

  public AbstractMapper(String id, int priorityValue) {
    this.id = id;
    this.priorityValue = priorityValue;
  }

  public AbstractMapper(String id) {
    this.id = id;
  }

  public void setTaskGraphParser(TaskGraphParser taskGraphParser) {
    this.taskGraphParser = taskGraphParser;
  }

  public int getPriorityValue() {
    return priorityValue;
  }

  public void setPriorityValue(int priorityValue) {
    this.priorityValue = priorityValue;
  }

  public abstract void execute();

  public abstract void execute(AbstractMapper mapper);

  public void run() {
    try {
      this.execute();
      //this.taskGraphParser.notifyDone (this); //for notifying the tasks successfully executed
    } catch (Exception ne) {
      ne.printStackTrace();
    }
  }
}

