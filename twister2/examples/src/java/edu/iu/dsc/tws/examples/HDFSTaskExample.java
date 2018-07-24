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
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.LinkedQueue;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.tsched.datalocalityaware.DataLocalityAwareTaskScheduling;
import edu.iu.dsc.tws.tsched.firstfit.FirstFitTaskScheduling;
import edu.iu.dsc.tws.tsched.roundrobin.RoundRobinTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class HDFSTaskExample implements IContainer {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final Logger LOG = Logger.getLogger(HDFSTaskExample.class.getName());
  private DataFlowOperation direct;
  private TaskExecutorFixedThread taskExecutor;
  private Status status;
  private TaskSchedulePlan taskSchedulePlan = null;

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("hdfstask-example");
    jobBuilder.setContainerClass(HDFSTaskExample.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 3);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  /**
   * This method initialize the config, container id, and resource plan objects.
   */
  public void init(Config cfg, int containerId, ResourcePlan plan) {

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

    GraphBuilder graphBuilder = GraphBuilder.newBuilder();
    graphBuilder.addTask("task1", taskMapper);
    graphBuilder.addTask("task2", taskReducer);
    graphBuilder.addTask("task3", taskShuffler);
    graphBuilder.addTask("task4", taskMerger);

    graphBuilder.connect("task1", "task2", "Reduce");
    graphBuilder.connect("task1", "task3", "Shuffle");
    graphBuilder.connect("task2", "task3", "merger1");
    graphBuilder.connect("task3", "task4", "merger2");

    graphBuilder.setParallelism("task1", 2);
    graphBuilder.setParallelism("task2", 2);
    graphBuilder.setParallelism("task3", 1);
    graphBuilder.setParallelism("task4", 1);

    graphBuilder.addConfiguration("task1", "Ram", GraphConstants.taskInstanceRam(cfg));
    graphBuilder.addConfiguration("task1", "Disk", GraphConstants.taskInstanceDisk(cfg));
    graphBuilder.addConfiguration("task1", "Cpu", GraphConstants.taskInstanceCpu(cfg));

    graphBuilder.addConfiguration("task2", "Ram", GraphConstants.taskInstanceRam(cfg));
    graphBuilder.addConfiguration("task2", "Disk", GraphConstants.taskInstanceDisk(cfg));
    graphBuilder.addConfiguration("task2", "Cpu", GraphConstants.taskInstanceCpu(cfg));

    graphBuilder.addConfiguration("task3", "Ram", GraphConstants.taskInstanceRam(cfg));
    graphBuilder.addConfiguration("task3", "Disk", GraphConstants.taskInstanceDisk(cfg));
    graphBuilder.addConfiguration("task3", "Cpu", GraphConstants.taskInstanceCpu(cfg));

    graphBuilder.addConfiguration("task4", "Ram", GraphConstants.taskInstanceRam(cfg));
    graphBuilder.addConfiguration("task4", "Disk", GraphConstants.taskInstanceDisk(cfg));
    graphBuilder.addConfiguration("task4", "Cpu", GraphConstants.taskInstanceCpu(cfg));

    List<String> datasetList = new ArrayList<>();

    datasetList.add("dataset1.txt");
    graphBuilder.addConfiguration("task1", "inputdataset", datasetList);

    datasetList = new ArrayList<>();
    datasetList.add("dataset2.txt");
    graphBuilder.addConfiguration("task2", "inputdataset", datasetList);

    datasetList = new ArrayList<>();
    datasetList.add("dataset3.txt");
    graphBuilder.addConfiguration("task3", "inputdataset", datasetList);

    datasetList = new ArrayList<>();
    datasetList.add("dataset4.txt");
    graphBuilder.addConfiguration("task4", "inputdataset", datasetList);

    /*List<String> datasetList = new ArrayList<>();

    graphBuilder.addConfiguration("task1", "dataset", datasetList.add("dataset1.txt"));
    graphBuilder.addConfiguration("task2", "dataset", datasetList.add("dataset2.txt"));
    graphBuilder.addConfiguration("task3", "dataset", datasetList.add("dataset3.txt"));
    graphBuilder.addConfiguration("task4", "dataset", datasetList.add("dataset4.txt"));*/

    /*graphBuilder.addConfiguration("task2", "dataset1", "dataset2.txt");
    graphBuilder.addConfiguration("task3", "dataset1", "dataset3.txt");
    graphBuilder.addConfiguration("task4", "dataset1", "dataset4.txt");*/

    WorkerPlan workerPlan = new WorkerPlan();
    Worker worker0 = new Worker(0);
    Worker worker1 = new Worker(1);
    Worker worker2 = new Worker(2);

    worker0.setCpu(4);
    worker0.setDisk(4000);
    worker0.setRam(2048);
    worker0.addProperty("bandwidth", 1000.0);
    worker0.addProperty("latency", 0.1);

    worker1.setCpu(4);
    worker1.setDisk(4000);
    worker1.setRam(2048);
    worker1.addProperty("bandwidth", 2000.0);
    worker1.addProperty("latency", 0.1);

    worker2.setCpu(4);
    worker2.setDisk(4000);
    worker2.setRam(2048);
    worker2.addProperty("bandwidth", 3000.0);
    worker2.addProperty("latency", 0.1);

    workerPlan.addWorker(worker0);
    workerPlan.addWorker(worker1);
    workerPlan.addWorker(worker2);

    DataFlowTaskGraph dataFlowTaskGraph = graphBuilder.build();

    if (containerId == 0) {
      //Twister2 -> HDFS Integration Testing....
      //Move the code to the actual task to perform the processing.
      String srcFileLocation = null;
      String pathString = null;
      Configuration conf = new Configuration(false);

      LOG.info("Hadoop Home directory is:" + HdfsDataContext.getHadoopHome(cfg));
      LOG.info("Config Directory:" + HdfsDataContext.getHdfsConfigDirectory(cfg));

      conf.addResource(new org.apache.hadoop.fs.Path(
          HdfsDataContext.getHdfsConfigDirectory(cfg)));

      //creating a directory using Twister2 API -> HDFS
      org.apache.hadoop.fs.FileSystem hadoopFileSystem = null;
      String directoryString;
      try {
        String directoryName = "/user/kannan";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + directoryName;
        hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
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

      //creating a directory using Twister2 API -> HDFS
      HadoopFileSystem hadoopFileSystem1 = null;
      try {
        String directoryName = "/user/kgovind";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + directoryName;
        hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        hadoopFileSystem1 = new HadoopFileSystem(conf, hadoopFileSystem);
        Path path = new Path(directoryString);
        hadoopFileSystem1.mkdirs(path);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          hadoopFileSystem1.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      //Creating a file, reading from a file and writing to the file using Twister2 API -> HDFS
      FSDataOutputStream hadoopDataOutputStream = null;
      InputStream in = null;
      HadoopFileSystem hadoopFileSystem2 = null;
      HadoopDataOutputStream hadoopDataOutputStream1 = null;
      try {
        String fileName = "/user/kannan/dataset1.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        File sourceFileObj = new File("/home/kgovind/hadoop-2.9.0/etc/hadoop/hadoop-env.sh");
        if (sourceFileObj.exists()) {
          if (!hadoopFileSystem2.exists(path)) {
            hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
            in = new BufferedInputStream(new FileInputStream(sourceFileObj));
            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
              hadoopDataOutputStream1.write(b, 0, numBytes);
            }
          }
        } else {
          System.out.println("File already exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (in != null) {
            in.close();
          }
          if (hadoopDataOutputStream1 != null) {
            hadoopDataOutputStream1.close();
          }
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kannan/dataset2.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        File sourceFileObj = new File("/home/kgovind/hadoop-2.9.0/etc/hadoop/hadoop-env.sh");
        if (sourceFileObj.exists()) {
          org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
          if (!hadoopFileSystem.exists(path)) {
            hadoopDataOutputStream = hadoopFileSystem.create(path);
            for (int i = 0; i < 10; i++) {
              hadoopDataOutputStream.write(
                  "Hello HDFS Data Output Stream\n".getBytes(DEFAULT_CHARSET));
            }
          }
        } else {
          System.out.println("File already exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem != null) {
            hadoopFileSystem.close();
            //hadoopDataOutputStream.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }


      //Creating a file, reading from a file and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kannan/dataset3.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        File sourceFileObj = new File("/home/kgovind/hadoop-2.9.0/etc/hadoop/hadoop-env.sh");
        if (sourceFileObj.exists()) {
          if (!hadoopFileSystem2.exists(path)) {
            hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
            in = new BufferedInputStream(new FileInputStream(sourceFileObj));
            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
              hadoopDataOutputStream1.write(b, 0, numBytes);
            }
          }
        } else {
          System.out.println("File already exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (in != null) {
            in.close();
          }
          if (hadoopDataOutputStream1 != null) {
            hadoopDataOutputStream1.close();
          }
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kannan/dataset4.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        File sourceFileObj = new File("/home/kgovind/hadoop-2.9.0/etc/hadoop/hadoop-env.sh");
        if (sourceFileObj.exists()) {
          org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directoryString);
          if (!hadoopFileSystem.exists(path)) {
            hadoopDataOutputStream = hadoopFileSystem.create(path);
            for (int i = 0; i < 10; i++) {
              hadoopDataOutputStream.write(
                  "Hello HDFS Data Output Stream\n".getBytes(DEFAULT_CHARSET));
            }
          }
        } else {
          System.out.println("File already exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem != null) {
            hadoopFileSystem.close();
            //hadoopDataOutputStream.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }


      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kgovind/dataset1.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (!hadoopFileSystem2.exists(path)) {
          hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream1.write(
                "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
        } else {
          LOG.info("File exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
            hadoopDataOutputStream1.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kgovind/dataset2.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (!hadoopFileSystem2.exists(path)) {
          hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream1.write(
                "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
        } else {
          LOG.info("File exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
            hadoopDataOutputStream1.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kgovind/dataset3.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (!hadoopFileSystem2.exists(path)) {
          hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream1.write(
                "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
        } else {
          LOG.info("File exists in hdfs");
          return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
            hadoopDataOutputStream1.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Creating and writing to the file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kgovind/dataset4.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (!hadoopFileSystem2.exists(path)) {
          hadoopDataOutputStream1 = hadoopFileSystem2.create(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream1.write(
                "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
        } else if (hadoopFileSystem2.exists(path))  {
          hadoopDataOutputStream1 = hadoopFileSystem2.append(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream1.write(
                "Hi, I am appending to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
          LOG.info("File exists in hdfs");
          //return;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
            hadoopDataOutputStream1.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Reading a file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kgovind/dataset2.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (!hadoopFileSystem2.exists(path)) {
          BufferedReader br = new BufferedReader(new InputStreamReader(
              hadoopFileSystem2.open(path)));
          String line;
          line = br.readLine();
          while (line != null) {
            //System.out.println(line);
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
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Reading a file using Twister2 API -> HDFS
      try {
        String fileName = "/user/kannan/dataset1.txt";
        directoryString = HdfsDataContext.getHdfsUrlDefault(cfg) + fileName;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(directoryString);
        if (hadoopFileSystem2.exists(path)) {
          BufferedReader br = new BufferedReader(new InputStreamReader(
              hadoopFileSystem2.open(path)));
          String line;
          line = br.readLine();
          while (line != null) {
            //System.out.println(line);
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
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Retrieve file status using Twister2 API -> HDFS
      try {
        srcFileLocation = "/user/kannan/dataset1.txt";
        pathString = HdfsDataContext.getHdfsUrlDefault(cfg) + srcFileLocation;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(pathString);
        LOG.info(String.format("Hadoop File System Status:"
            + hadoopFileSystem2.getFileStatus(path)));
        LOG.info(String.format("Block Size:"
            + hadoopFileSystem2.getFileStatus(path).getBlockSize()));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }


      //Deleting a directory using Twister2 API -> HDFS
      // Commented for testing...it is a working code
     /* try {
        srcFileLocation = "/user/kgovind";
        pathString = HdfsDataContext.getHdfsUrlDefault(cfg) + srcFileLocation;
        hadoopFileSystem2 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(pathString);
        if (hadoopFileSystem2.exists(path)) {
          hadoopFileSystem2.delete(path, true);
        }
      } catch (NullPointerException e) {
        e.printStackTrace();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } finally {
        try {
          if (hadoopFileSystem2 != null) {
            hadoopFileSystem2.close();
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }*/

      if (dataFlowTaskGraph != null) {
        LOG.info("Task Scheduling Mode:" + TaskSchedulerContext.taskSchedulingMode(cfg));
        if (TaskSchedulerContext.taskSchedulingMode(cfg).equals("roundrobin")) {
          RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
          roundRobinTaskScheduling.initialize(cfg);
          taskSchedulePlan = roundRobinTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);

        } else if (TaskSchedulerContext.taskSchedulingMode(cfg).equals("firstfit")) {
          FirstFitTaskScheduling firstFitTaskScheduling = new FirstFitTaskScheduling();
          firstFitTaskScheduling.initialize(cfg);
          taskSchedulePlan = firstFitTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);

        } else if (TaskSchedulerContext.taskSchedulingMode(cfg).equals("datalocalityaware")) {
          DataLocalityAwareTaskScheduling dataLocalityAwareTaskScheduling = new
              DataLocalityAwareTaskScheduling();
          dataLocalityAwareTaskScheduling.initialize(cfg);
          taskSchedulePlan = dataLocalityAwareTaskScheduling.schedule(
              dataFlowTaskGraph, workerPlan);
        }
        try {
          if (taskSchedulePlan.getContainersMap() != null) {
            LOG.info("Task schedule plan details:"
                + taskSchedulePlan.getTaskSchedulePlanId() + ":"
                + taskSchedulePlan.getContainersMap());
          }
        } catch (NullPointerException ne) {
          ne.printStackTrace();
        }
      }
    }

    System.out.println("I am entering the HDFS Data Task Example");
    System.out.println("HDFS Data Context:" + HdfsDataContext.getHdfsClassDefault(cfg));
    System.out.println("HDFS Implementation Key:" + HdfsDataContext.getHdfsImplementationKey(cfg));
    System.out.println("HDFS Config Directory:" + HdfsDataContext.getHdfsConfigDirectory(cfg));
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
    private TaskContext ctx;
    private Config config;

    protected TaskMapper(String taskName1) {
      this.taskName = taskName1;
    }


    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      LOG.info("Input Files:" + cfg.getListValue("dataset"));
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

      //ctx.write("partition-edge", "Hello");
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
      LOG.info("Input Files:" + cfg.getListValue("dataset"));
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
      LOG.info("Input Files:" + cfg.getListValue("dataset"));
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
      LOG.info("Input Files:" + cfg.getListValue("dataset"));
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
