package edu.iu.dsc.tws.task.taskgraphbuilder;

public abstract class TaskMapper implements Runnable {

  private final String taskId;
  private float taskExecutionWeight = 0;
  private int taskPriority;

  protected TaskMapper(String id, int taskpriority) {
    this.taskId = id;
    this.taskPriority = taskpriority;
  }

  protected TaskMapper(String id) {
    this.taskId = id;
  }

  public int getTaskPriority() {
    return taskPriority;
  }

  public void setTaskPriority(int taskpriority) {
    this.taskPriority = taskpriority;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskMapper)) {
      return false;
    }
    TaskMapper taskMapper = (TaskMapper) o;
    return taskId != null ? taskId.equals(taskMapper.taskId) : taskMapper.taskId == null;
  }

  @Override
  public int hashCode() {
    return taskId != null ? taskId.hashCode() : 0;
  }

  public float getExecutionWeight() {
    return taskExecutionWeight;
  }

  public void setExecutionWeight(float executionWeight) {
    this.taskExecutionWeight = executionWeight;
  }

  public boolean hasExecutionWeight() {
    return this.taskExecutionWeight != -1;
  }

  public abstract void execute();

  public abstract void execute(TaskMapper taskMapper);

  public abstract void execute(String message);

  public void run() {
    try {
      this.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
