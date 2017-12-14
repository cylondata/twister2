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

  public abstract void execute(String message);

  public void run() {
    try {
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
