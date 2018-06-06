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
package edu.iu.dsc.tws.examples;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class HDFSTaskExample implements IContainer {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final Logger LOG = Logger.getLogger(SimpleTGraphExample1.class.getName());
  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  private Status status;
  private TaskSchedulePlan taskSchedulePlan = null;

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("hdfstask-example");
    jobBuilder.setContainerClass(HDFSTaskExample.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 1);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  /**
   * This method initialize the config, container id, and resource plan objects.
   */
  public void init(Config cfg, int containerId, ResourcePlan plan) {

    String srcFileLocation = "user/hdfs-site.xml";
    String pathString = String.format("hdfs://hairy.soic.indiana.edu:9000/%s", srcFileLocation);

    Configuration conf = new Configuration(false);
    LOG.info("Config Directory:%%%%%%%%%%%%"
        + HdfsDataContext.getHdfsConfigDirectory(cfg));

    /*conf.addResource(new org.apache.hadoop.fs.Path(
        HdfsDataContext.getHdfsConfigDirectory(cfg)));*/

    conf.addResource(new org.apache.hadoop.fs.Path(
        "/home/kgovind/hadoop-2.9.0/etc/hadoop/core-site.xml"));

    //creating a directory
    org.apache.hadoop.fs.FileSystem hadoopFileSystem = null;
    String directoryString;
    try {
      String directoryName = "user/kannan";
      directoryString = "hdfs://hairy.soic.indiana.edu:9000/" + directoryName;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
      LOG.info("%%%%%%%%%%%% Path Uri Is:" + path);
      hadoopFileSystem.mkdirs(path);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        hadoopFileSystem.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    //Creating a directory using Twister2-> HadoopFile System
    HadoopFileSystem hadoopFileSystem1 = null;
    try {
      String directoryName = "user/test";
      directoryString = "hdfs://hairy.soic.indiana.edu:9000/" + directoryName;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      hadoopFileSystem1 = new HadoopFileSystem(conf, hadoopFileSystem);
      LOG.info(String.format("Conf:" + hadoopFileSystem + "::::" + hadoopFileSystem1));
      Path path = new Path(directoryString);
      //hadoopFileSystem1.mkdirs(path);
      hadoopFileSystem.mkdirs(HadoopFileSystem.toHadoopPath(path));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        hadoopFileSystem.close();
        //hadoopFileSystem1.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    //Creating a file and writing to the file
    FSDataOutputStream hadoopDataOutputStream = null;
    //HadoopDataOutputStream hadoopDataOutputStream = null;
    //FSDataOutputStream fsDataOutputStream = null;
    InputStream in = null;
    try {
      String fileName = "user/kannan/test.xml";
      directoryString = "hdfs://hairy.soic.indiana.edu:9000/" + fileName;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      File sourceFileObj = new File("/home/kgovind/hadoop-2.9.0/etc/hadoop/hadoop-env.sh");
      if (sourceFileObj.exists()) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
        if (!hadoopFileSystem.exists(path)) {

          /*hadoopDataOutputStream = new HadoopDataOutputStream(fsDataOutputStream);
          hadoopDataOutputStream.getHadoopOutputStream();*/
          hadoopDataOutputStream = hadoopFileSystem.create(path);
          in = new BufferedInputStream(new FileInputStream(sourceFileObj));
          byte[] b = new byte[1024];
          int numBytes = 0;
          while ((numBytes = in.read(b)) > 0) {
            hadoopDataOutputStream.write(b, 0, numBytes);
          }
        }
      } else {
        System.out.println("File already exists in hdfs");
        return;
      }
      /*for (int i = 0; i < 10; i++) {
        hadoopDataOutputStream.write("Hello HDFS Data Output Stream\n".getBytes(DEFAULT_CHARSET));
      }
      hadoopDataOutputStream.close();*/
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      try {
        if (in != null) {
          in.close();
        }
        if (hadoopDataOutputStream != null) {
          hadoopDataOutputStream.close();
        }
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    //Reading a file form hdfs directory
    try {
      String fileName = "user/core-site.xml";
      directoryString = "hdfs://hairy.soic.indiana.edu:9000/" + fileName;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      LOG.info("%%%%%%%%%%%% Hadoop File System:" + hadoopFileSystem);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
      if (hadoopFileSystem.exists(path)) {
        BufferedReader br = new BufferedReader(new InputStreamReader(
            hadoopFileSystem.open(path)));
        String line;
        line = br.readLine();
        while (line != null) {
          System.out.println(line);
          line = br.readLine();
        }
        br.close();
      } else {
        System.out.println("File does not exist on HDFS");
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    //File Status
    try {
      srcFileLocation = "user/kannan/test.xml";
      pathString = "hdfs://hairy.soic.indiana.edu:9000/" + srcFileLocation;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathString);
      LOG.info(String.format("Hadoop File System Status:"
          + hadoopFileSystem.getFileStatus(path)));
      LOG.info(String.format("Block Size:"
          + hadoopFileSystem.getFileStatus(path).getBlockSize()));
    } catch (Exception e) {
      e.printStackTrace();
    }

    //Deleting a directory
    try {
      srcFileLocation = "user/test";
      pathString = "hdfs://hairy.soic.indiana.edu:9000/" + srcFileLocation;
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathString);
      if (hadoopFileSystem.exists(path)) {
        hadoopFileSystem.delete(path, true);
      }
    } catch (NullPointerException e) {
      e.printStackTrace();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    System.out.println("I am entering the HDFS Data Task Example");
    System.out.println("HDFS Data Context:" + HdfsDataContext.getHdfsClassDefault(cfg));
    System.out.println("HDFS Implementation Key:" + HdfsDataContext.getHdfsImplementationKey(cfg));
    System.out.println("HDFS Config Directory:" + HdfsDataContext.getHdfsConfigDirectory(cfg));

    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    taskExecutor = new TaskExecutorFixedThread();
    this.status = Status.INIT;

    TaskPlan taskPlan = Utils.createTaskPlan(cfg, plan);
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);
    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    sources.add(0);
    int destination = 1;

    Map<String, Object> newCfg = new HashMap<>();
    LinkedQueue<IMessage> pongQueue = new LinkedQueue<IMessage>();
    taskExecutor.registerQueue(0, pongQueue);

    direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
        destination, new PingPongReceive());
    taskExecutor.initCommunication(channel, direct);

    TaskMapper taskMapper = new TaskMapper("task1");
    TaskReducer taskReducer = new TaskReducer("task2");
    TaskShuffler taskShuffler = new TaskShuffler("task3");
    TaskMerger taskMerger = new TaskMerger("task4");

  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int[] d = new int[10];
    for (int i = 0; i < 10; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public WorkerPlan createWorkerPlan(ResourcePlan resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (ResourceContainer resource : resourcePlan.getContainers()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private class TaskMapper implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMapper(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, TaskContext collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }
  }

  private class TaskReducer implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskReducer(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, TaskContext collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }
  }

  private class TaskShuffler implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskShuffler(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, TaskContext collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }
  }

  private class TaskMerger implements ITask {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMerger(String taskName1) {
      this.taskName = taskName1;
    }

    /**
     * Prepare the task to be executed
     *
     * @param cfg the configuration
     * @param collection the output collection
     */
    @Override
    public void prepare(Config cfg, TaskContext collection) {

    }

    /**
     * Code that needs to be executed in the Task
     */
    @Override
    public IMessage execute() {
      return null;
    }

    /**
     * Code that is executed for a single message
     */
    @Override
    public IMessage execute(IMessage content) {
      return null;
    }

    /**
     * Execute with an incoming message
     */
    @Override
    public void run(IMessage content) {

    }

    /**
     * Execute without an incoming message
     */
    @Override
    public void run() {

    }
  }

  private class PingPongReceive implements MessageReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;
      if (count % 10000 == 0) {
        LOG.info("received message: " + count);
      }
      if (count == 100000) {
        status = Status.LOAD_RECEIVE_FINISHED;
      }
      return true;
    }

    @Override
    public void progress() {

    }
  }
}
