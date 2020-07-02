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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

/**
 * A word count where we read text files with words
 */
public class FileBasedWordCount implements Twister2Worker, Serializable {
  private static final Logger LOG = Logger.getLogger(FileBasedWordCount.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchTSetEnvironment env = TSetEnvironment.initBatch(workerEnv);

    int sourcePar = (int) env.getConfig().get("PAR");

    // read the file line by line by using a single worker
    SourceTSet<String> lines = env.createSource(new WordCountFileSource(), 1);

    // distribute the lines among the workers and performs a flatmap operation to extract words
    ComputeTSet<String, Iterator<String>> words =
        lines.partition(new HashingPartitioner<>(), sourcePar)
            .flatmap((FlatMapFunc<String, String>) (l, collector) -> {
              StringTokenizer itr = new StringTokenizer(l);
              while (itr.hasMoreTokens()) {
                collector.collect(itr.nextToken());
              }
            });

    // attach count as 1 for each word
    KeyedTSet<String, Integer> groupedWords = words.mapToTuple(w -> new Tuple<>(w, 1));

    // performs reduce by key at each worker
    KeyedReduceTLink<String, Integer> keyedReduce = groupedWords.keyedReduce(Integer::sum);

    // gather the results to worker0 (there is a dummy map op here to pass the values to edges)
    // and write to a file
    keyedReduce.map(i -> i).gather().forEach(new WordcountFileWriter());
  }

  static class WordCountFileSource extends BaseSourceFunc<String> {
    private DataSource<String, FileInputSplit<String>> dataSource;
    private InputSplit<String> dataSplit;

    @Override
    public void prepare(TSetContext context) {
      super.prepare(context);
      String inputFile = (String) context.getConfig().get("INPUT_FILE");
      this.dataSource = new DataSource<>(context.getConfig(),
          new LocalTextInputPartitioner(new Path(inputFile), context.getParallelism()),
          context.getParallelism());
      this.dataSplit = this.dataSource.getNextSplit(context.getIndex());
    }

    @Override
    public boolean hasNext() {
      try {
        if (dataSplit == null || dataSplit.reachedEnd()) {
          dataSplit = dataSource.getNextSplit(getTSetContext().getIndex());
        }
        return dataSplit != null && !dataSplit.reachedEnd();
      } catch (IOException e) {
        throw new RuntimeException("Unable read data split!");
      }
    }

    @Override
    public String next() {
      try {
        return dataSplit.nextRecord(null);
      } catch (IOException e) {
        throw new RuntimeException("Unable read data split!");
      }
    }
  }

  static class WordcountFileWriter extends BaseApplyFunc<Tuple<String, Integer>> {
    private BufferedWriter writer;

    @Override
    public void prepare(TSetContext ctx) {
      super.prepare(ctx);
      try {
        String file = ctx.getConfig().get("OUTPUT_FILE") + "." + getTSetContext().getIndex();
        File outFile = new File(file);
        outFile.getParentFile().mkdirs();

        writer = new BufferedWriter(new FileWriter(outFile, false));
      } catch (IOException e) {
        throw new RuntimeException("Unable to create file writer!");
      }
    }

    @Override
    public void apply(Tuple<String, Integer> data) {
      try {
        writer.write(data.getKey() + " " + data.getValue());
        writer.newLine();
      } catch (IOException e) {
        throw new RuntimeException("Unable to write!");
      }
    }

    @Override
    public void close() {
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close the writer!");
      }
    }
  }

  private static void downloadFile(String dest) {
    InputStream in;
    try {
      in = new URL("https://www.gutenberg.org/files/1342/1342-0.txt").openStream();
      Files.copy(in, Paths.get(dest), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    LOG.log(Level.INFO, "Starting wordcount Job");

    Options options = new Options();
    options.addOption("input", true, "Input file");
    options.addOption("output", true, "Output file");
    options.addOption("parallelism", true, "Parallelism");
    options.addOption("validate", true, "validate?");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    String input = cmd.getOptionValue("input");
    if (input == null) {
      LOG.warning("No input is provided! Downloading Pride and Prejudice text from "
          + "Gutenberg project");
      input = "/tmp/wc/wordcount.in";
      downloadFile(input);
    }

    String output = cmd.getOptionValue("output", "/tmp/wc/wordcount.out");
    int par = Integer.parseInt(cmd.getOptionValue("parallelism", "4"));
    boolean validate = Boolean.parseBoolean(cmd.getOptionValue("validate", "true"));

    LOG.info("Wordcount input: " + input + " output: " + output + " parallelism: " + par);

    long start = System.nanoTime();

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("INPUT_FILE", input);
    jobConfig.put("OUTPUT_FILE", output);
    jobConfig.put("PAR", par);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("tset-wordcount");
    jobBuilder.setWorkerClass(FileBasedWordCount.class);
    jobBuilder.addComputeResource(1, 512, par);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());

    LOG.info("time elapsed ms " + (System.nanoTime() - start) * 1e-6);

    // perform validation
    if (validate) {
      LOG.info("validating results!");
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
}
