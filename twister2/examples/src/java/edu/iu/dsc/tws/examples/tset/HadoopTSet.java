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
package edu.iu.dsc.tws.examples.tset;

import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class HadoopTSet implements IWorker {
  private static final Logger LOG = Logger.getLogger(HadoopTSet.class.getName());
  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    WorkerEnvironment workerEnv = WorkerEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    BatchTSetEnvironment tSetEnv = TSetEnvironment.initBatch(workerEnv);

    Configuration configuration = new Configuration();

    configuration.addResource(
        new Path("/home/skamburu/deploy/Hadoop/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
    configuration.addResource(
        new Path("/home/skamburu/deploy/Hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    configuration.addResource(
        new Path("/home/skamburu/deploy/Hadoop/hadoop-2.6.0/etc/hadoop/yarn-site.xml"));
    configuration.addResource(
        new Path("/home/skamburu/deploy/Hadoop/hadoop-2.6.0/etc/hadoop/mapred-site.xml"));
    configuration.set(TextInputFormat.INPUT_DIR, "/input");
    SourceTSet<Tuple<LongWritable, Text>> source =
        tSetEnv.createHadoopSource(configuration, TextInputFormat.class, 4);

    source.direct().forEach(new ApplyFunc<Tuple<LongWritable, Text>>() {
      @Override
      public void apply(Tuple<LongWritable, Text> data) {
        LOG.info(data.getKey() + " : " + data.getValue());
      }
    });
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    JobConfig jobConfig = new JobConfig();
    submitJob(config, 4, jobConfig, HadoopTSet.class.getName());
  }

  private static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(clazz)
        .setWorkerClass(clazz)
        .addComputeResource(1, 512, containers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
