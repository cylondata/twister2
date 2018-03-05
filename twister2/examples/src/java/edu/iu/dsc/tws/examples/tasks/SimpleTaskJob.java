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
package edu.iu.dsc.tws.examples.tasks;

import java.util.ArrayList;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.LinkedQueue;

import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;

/**
 * Created by vibhatha on 3/2/18.
 */
public class SimpleTaskJob {

  static {
    System.setProperty("java.util.logging.SimpleFormatter.format",
        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
  }

  private static final Logger LOG = Logger.getLogger(SimpleTaskJob.class.getName());

  public void init() {
    TaskExecutorFixedThread taskExecutor = new TaskExecutorFixedThread();
    taskExecutor.setTaskMessageProcessLimit(10);
    LOG.info("Task 0 Registering");
    ArrayList<Integer> inputQueue0 = new ArrayList<>();
    ArrayList<Integer> outputQueue0 = new ArrayList<>();

    inputQueue0.add(0);
    outputQueue0.add(0);

    LinkedQueue<Message> pongQueue = new LinkedQueue<Message>();

    taskExecutor.registerQueue(0, pongQueue);
    taskExecutor.registerTask(new SendTask(0), inputQueue0, outputQueue0);
    taskExecutor.submitTask(0);
    taskExecutor.submitMessage(0, "Hi 0");

    //taskExecutor.submitMessage(0,"Hi Executor 0");

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    LOG.info("Task 1 Registering");
    ArrayList<Integer> inputQueue1 = new ArrayList<>();
    ArrayList<Integer> outputQueue1 = new ArrayList<>();
    taskExecutor.registerTask(new ReceiveTask(1), inputQueue1, outputQueue1);
    taskExecutor.submitTask(1);
    taskExecutor.submitMessage(0, "Hi 1");
    Set<Integer> runningTasks = taskExecutor.getRunningTasks();
    LOG.info("Running Tasks : " + runningTasks.size());
    for (Integer task: runningTasks) {
      LOG.info("Task Running : id : " + task);
    }
  }


  public class MapWorker implements Runnable {
    private int task = 0;
    private int sendCount = 0;
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {

      System.out.println("Hello MapWorker");

    }
  }

  public class SendTask extends Task {

    private int taskId = 0;
    public SendTask(int id) {
      this.taskId = id;
      this.setTaskId(id);
    }

    @Override
    public Message execute() {
      LOG.info("#########################################");
      LOG.info("Send Task Execute");

      TaskMessage<String> taskMessage = new TaskMessage<>("Task Message : " + this.getTaskId());
      LOG.info("#########################################");
      return taskMessage;
    }

    @Override
    public Message execute(Message content) {

      LOG.info("Message : " + content.toString());

      return content;
    }
  }

  public class  ReceiveTask extends Task {

    private int taskid = 1;
    public ReceiveTask(int id) {
      this.taskid = id;
      this.setTaskId(id);
    }

    @Override
    public Message execute() {
      LOG.info("#########################################");
      LOG.info("Receive Task Execute");
      TaskMessage<String> taskMessage = new TaskMessage<>("Task Message : " + this.getTaskId());
      LOG.info("#########################################");
      return taskMessage;
    }

    @Override
    public Message execute(Message content) {
      LOG.info("Message : " + content.toString());
      return null;
    }
  }

  public static void main(String[] args) {

    SimpleTaskJob simpleTaskJob = new SimpleTaskJob();
    simpleTaskJob.init();


  }

}
