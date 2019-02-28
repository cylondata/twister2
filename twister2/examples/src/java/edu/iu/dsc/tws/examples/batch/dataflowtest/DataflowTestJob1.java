package edu.iu.dsc.tws.examples.batch.dataflowtest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.dataobjects.DataObjectConstants;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class DataflowTestJob1 extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(DataflowTestJob1.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    SourceTask sourceTask = new SourceTask();
    ReduceTask reduceTask = new ReduceTask();
    ComputeTask computeTask = new ComputeTask();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);

    int parallel = Integer.parseInt(DataObjectConstants.ARGS_PARALLELISM_VALUE);
    LOG.info("parallelism value:" + parallel);

    builder.addSource("source", sourceTask, 4);

    /*ComputeConnection computeConnection = builder.addCompute("compute", computeTask, 4);
    computeConnection.direct("source", "direct", DataType.OBJECT);*/

    ComputeConnection computeConnection = builder.addCompute("compute", computeTask, 4);
    computeConnection.allreduce("source", "all-reduce1", new Aggregator(), DataType.OBJECT);

    ComputeConnection rc = builder.addSink("sink", reduceTask, 4);
    rc.allreduce("compute", "all-reduce2",
        new Aggregator(), DataType.OBJECT);

    builder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      taskExecutor.execute(graph, plan);
      LOG.log(Level.INFO, "iteration done:" + i);
    }
    long stopTime = System.currentTimeMillis();
    long executionTime = stopTime - startTime;
    LOG.info("Total Execution Time to Complete K-Means consists of"
        + "\t" + executionTime + "(in milliseconds)");
  }

  private static class SourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;

    @Override
    public void execute() {
      int seedValue = 100;
      datapoints = new double[numPoints];
      Random r = new Random(seedValue);
      for (int i = 0; i < numPoints; i++) {
        double randomValue = r.nextDouble();
        datapoints[i] = randomValue;
      }
      context.writeEnd("all-reduce", datapoints);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      numPoints = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DSIZE));
    }
  }

  private static class ComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
          + ":" + context.taskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("all-reduce", it.next());
        }
      }
      LOG.info(String.format("%d %d All-Reduce Received count: %d", context.getWorkerId(),
          context.taskId(), count));
      context.end("all-reduce");
      return true;
    }
  }

  private static class ReduceTask extends BaseSink {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
          + ":" + context.taskId());
      datapoints = (double[]) message.getContent();
      return true;
    }
  }

  public class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;

      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }

      //LOG.info("New double value is::::" + Arrays.toString(object31) + "\t" + object31.length);
      return object31;
    }
  }

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "Experiment Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(DataObjectConstants.ARGS_ITERATIONS, true, "iter");
    options.addOption(DataObjectConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.ARGS_WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.ARGS_DSIZE, true, "dsize");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_WORKERS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_PARALLELISM_VALUE));
    int iterations = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_ITERATIONS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_DSIZE));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("Experiment-job");
    jobBuilder.setWorkerClass(DataflowTestJob1.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
