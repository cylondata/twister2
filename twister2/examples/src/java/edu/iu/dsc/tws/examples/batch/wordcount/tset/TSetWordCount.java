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

package edu.iu.dsc.tws.examples.batch.wordcount.tset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSinkFunc;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class TSetWordCount implements BatchTSetIWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(TSetWordCount.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    int sourcePar = 4;
    Configuration configuration = new Configuration();

    configuration.set(TextInputFormat.INPUT_DIR, "file:///tmp/wc");

    SourceTSet<String> lines = env
        .createSource(new WordCountFileSource((String) env.getConfig().get("INPUT_FILE")), 1);

    ComputeTSet<String, Iterator<String>> words =
        lines.partition(new HashingPartitioner<>(), sourcePar)
            .flatmap((FlatMapFunc<String, String>) (l, collector) -> {
              StringTokenizer itr = new StringTokenizer(l);
              while (itr.hasMoreTokens()) {
                collector.collect(itr.nextToken());
              }
            });

    KeyedTSet<String, Integer> groupedWords = words.mapToTuple(w -> new Tuple<>(w, 1));

    KeyedReduceTLink<String, Integer> keyedReduce = groupedWords.keyedReduce(Integer::sum);

    ComputeTSet<Tuple<String, Integer>, Iterator<Tuple<String, Integer>>> map =
        keyedReduce.map(i -> i);

    map.gather().sink(new WordCountFileLogger((String) env.getConfig().get("OUTPUT_FILE")));
  }

  static class WordCountFileSource extends BaseSourceFunc<String> {

    private String inputFile;
    private DataSource<String, FileInputSplit<String>> dataSource;
    private InputSplit<String> dataSplit;

    WordCountFileSource(String inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public void prepare(TSetContext context) {
      super.prepare(context);

      // load the split
      this.dataSource = new DataSource<>(context.getConfig(),
          new LocalTextInputPartitioner(new Path(inputFile), context.getParallelism()),
          context.getParallelism());
      this.dataSplit = this.dataSource.getNextSplit(context.getIndex());
    }

    @Override
    public boolean hasNext() {
      try {
        if (dataSplit != null && !dataSplit.reachedEnd()) {
          return true;
        } else {
          dataSplit = dataSource.getNextSplit(getTSetContext().getIndex());
          if (dataSplit != null) {
            return true;
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }

    @Override
    public String next() {
      try {
        return dataSplit.nextRecord(null);
      } catch (IOException e) {
        e.printStackTrace();
      }

      return null;
    }
  }

  static class WordCountFileLogger extends
      BaseSinkFunc<Iterator<Tuple<Integer, Tuple<String, Integer>>>> {
    private BufferedWriter writer;
    private String fileName;

    WordCountFileLogger(String fname) {
      this.fileName = fname;
    }

    @Override
    public void prepare(TSetContext ctx) {
      super.prepare(ctx);
      String fileWithIdx = String.format("%s.%d", fileName, getTSetContext().getIndex());
      try {
        writer = new BufferedWriter(new FileWriter(fileWithIdx, false));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public boolean add(Iterator<Tuple<Integer, Tuple<String, Integer>>> value) {
      try {
        while (value.hasNext()) {
          Tuple<String, Integer> t = value.next().getValue();
          writer.write(t.getKey() + " " + t.getValue());
          writer.newLine();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return true;
    }

    @Override
    public void close() {
      try {
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException {

    String input = "/tmp/wc/wordcount.in";
    String output = "/tmp/wc/wordcount.out";
    long start = System.nanoTime();

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("INPUT_FILE", input);
    jobConfig.put("OUTPUT_FILE", output);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("tset-wordcount");
    jobBuilder.setWorkerClass(TSetWordCount.class);
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());

    LOG.info("time elapsed ms " + (System.nanoTime() - start) * 1e-6);

    // validate
    Map<String, Integer> trusted = new TreeMap<>();

    try (BufferedReader br = new BufferedReader(new FileReader(input))) {
      String line;
      while ((line = br.readLine()) != null) {
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
          String word = itr.nextToken();
          trusted.putIfAbsent(word, 0);
          int count = trusted.get(word);
          trusted.put(word, ++count);
        }
      }
    }


    Map<String, Integer> test1 = new TreeMap<>();
    try (BufferedReader br = new BufferedReader(new FileReader(output + ".0"))) {
      String line;
      while ((line = br.readLine()) != null && !line.isEmpty()) {
        String[] sp = line.split(" ");
        test1.put(sp[0].trim(), Integer.parseInt(sp[1]));
      }
    }

    for (Map.Entry<String, Integer> e : trusted.entrySet()) {
      int t = test1.get(e.getKey());
      if (t != e.getValue()) {
        LOG.severe(String.format("Expected: %s %d Got: %s %d", e.getKey(), e.getValue(),
            e.getKey(), t));
      }
    }

    if (test1.equals(trusted)) {
      LOG.info("RESULTS VALID!");
    } else {
      LOG.severe("UNSUCCESSFUL!");

      try (BufferedWriter br = new BufferedWriter(new FileWriter(output + ".trusted",
          false))) {
        for (Map.Entry<String, Integer> e : trusted.entrySet()) {
          br.write(String.format("%s %d\n", e.getKey(), e.getValue()));
        }
      }
    }
  }
}
