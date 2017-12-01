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

public abstract class Mapper implements Runnable {

  private final String id;
  private float executionWeight = -1;
  private int priorityValue;

  protected Mapper(String id, int priorityValue) {
    this.id = id;
    this.priorityValue = priorityValue;
  }

  protected Mapper(String id) {
    this.id = id;
  }

  public int getPriorityValue() {
    return priorityValue;
  }

  public void setPriorityValue(int priorityValue) {
    this.priorityValue = priorityValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mapper)) {
      return false;
    }
    Mapper mapper = (Mapper) o;
    return id != null ? id.equals(mapper.id) : mapper.id == null;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }

  public float getExecutionWeight() {
    return executionWeight;
  }

  public void setExecutionWeight(float executionWeight) {
    this.executionWeight = executionWeight;
  }

  public boolean hasExecutionWeight() {
    return this.executionWeight != -1;
  }

  public abstract void execute();

  public abstract void execute(Mapper mapper);

  public void run() {
    try {
      System.out.println("------------Starting Task-------------------" + this.id);
      this.execute();

      //this.taskGraphParser.notifyDone (this);
    } catch (Exception ne) {
      ne.printStackTrace();
    }
  }

  /*public void setTaskGraphParser(TaskGraphParser taskGraphParser){
        this.taskGraphParser = taskGraphParser;
  }*/
}
