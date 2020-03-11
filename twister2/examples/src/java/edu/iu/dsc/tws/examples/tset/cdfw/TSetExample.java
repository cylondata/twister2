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
package edu.iu.dsc.tws.examples.tset.cdfw;

import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.examples.batch.cdfw.CDFConstants;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;
import edu.iu.dsc.tws.tset.cdfw.BatchTSetBaseDriver;
import edu.iu.dsc.tws.tset.cdfw.BatchTSetCDFWEnvironment;
import edu.iu.dsc.tws.tset.links.batch.DirectTLink;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public final class TSetExample {
  private static final Logger LOG = Logger.getLogger(TSetExample.class.getName());

  private TSetExample() {
  }

  public static class Driver extends BatchTSetBaseDriver {

    @Override
    public void execute(BatchTSetCDFWEnvironment env) {
      SourceTSet<Integer> src = env.createSource(new FirstSourceFunc(), 2);

      DirectTLink<Integer> direct = src.direct().setName("direct");

      LOG.info("test foreach");
      direct.forEach(new ForeachFunc());
    }
  }

  private static class ForeachFunc implements ApplyFunc<Integer> {

    @Override
    public void apply(Integer data) {
      System.out.println(data);
    }
  }

  private static class FirstSourceFunc implements SourceFunc<Integer> {
    private int c = 0;

    @Override
    public boolean hasNext() {
      return c < 10;
    }

    @Override
    public Integer next() {
      return c++;
    }
  }

  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    Options options = new Options();
    options.addOption(CDFConstants.ARGS_PARALLELISM_VALUE, true, "2");
    options.addOption(CDFConstants.ARGS_WORKERS, true, "2");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int instances = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));

    configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(instances));
    configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setWorkerClass(CDFWWorker.class)
        .setJobName(TSetExample.class.getName())
        .setDriverClass(TSetExample.Driver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
