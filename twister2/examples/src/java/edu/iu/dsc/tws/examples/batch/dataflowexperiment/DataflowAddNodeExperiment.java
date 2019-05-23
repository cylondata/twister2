package edu.iu.dsc.tws.examples.batch.dataflowexperiment;

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
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
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

public class DataflowAddNodeExperiment extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(DataflowAddNodeExperiment.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    SourceTask sourceTask = new SourceTask();
    ReduceTask reduceTask = new ReduceTask();
    FirstComputeTask firstComputeTask = new FirstComputeTask();
    SecondComputeTask secondComputeTask = new SecondComputeTask();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    DataflowJobParameters dataflowJobParameters = new DataflowJobParameters().build(config);

    int parallel = dataflowJobParameters.getParallelismValue();
    int iter = dataflowJobParameters.getIterations();

    builder.addSource("source", sourceTask, parallel);
    ComputeConnection computeConnection
        = builder.addCompute("firstcompute", firstComputeTask, parallel);
    ComputeConnection computeConnection1
        = builder.addCompute("secondcompute", secondComputeTask, parallel);
    ComputeConnection rc = builder.addSink("sink", reduceTask, parallel);

    computeConnection.direct("source", "fdirect", DataType.OBJECT);
    computeConnection1.direct("firstcompute", "sdirect", DataType.OBJECT);
    rc.allreduce("secondcompute", "all-reduce", new Aggregator(), DataType.OBJECT);

    builder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = builder.build();

    //ExecutionPlan plan = taskExecutor.plan(graph);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iter; i++) {
      //taskExecutor.execute(graph, plan);
      LOG.info("Completed Iteration:" + i);
    }
    long stopTime = System.currentTimeMillis();
    long executionTime = stopTime - startTime;
    LOG.info("Total Execution Time to complete Dataflow Additional Node Experiment"
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
      context.writeEnd("fdirect", datapoints);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      numPoints = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
    }
  }

  private static class FirstComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received Points (First Compute Task): " + context.getWorkerId()
          + ":" + context.globalTaskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("sdirect", it.next());
        }
      }
      context.end("sdirect");
      return true;
    }
  }

  private static class SecondComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received Points (Second Compute Task): " + context.getWorkerId()
          + ":" + context.globalTaskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("all-reduce", it.next());
        }
      }
      context.end("all-reduce");
      return true;
    }
  }

  private static class ReduceTask extends BaseSink {
    private static final long serialVersionUID = -5190777711234234L;
    private double[] datapoints;

    @Override
    public boolean execute(IMessage message) {
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
      return object31;
    }
  }

  public class Aggregator1 implements IFunction {
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
      return object31;
    }
  }


  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(DataObjectConstants.ARGS_ITERATIONS, true, "iter");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.WORKERS, true, "workers");
    options.addOption(DataObjectConstants.DSIZE, true, "dsize");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));
    int iterations = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_ITERATIONS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.DSIZE, Integer.toString(dsize));
    jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(DataObjectConstants.ARGS_ITERATIONS, Integer.toString(iterations));
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("Experiment2");
    jobBuilder.setWorkerClass(DataflowAddNodeExperiment.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
