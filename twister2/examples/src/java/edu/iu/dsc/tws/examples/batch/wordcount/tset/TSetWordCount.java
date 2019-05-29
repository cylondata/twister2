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
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.tset.BaseIterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.BaseSink;
import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.GroupedTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableFlatMapTSet;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class TSetWordCount extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(TSetWordCount.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    int sourcePar = 1;
    int sinkPar = 1;

    BatchSourceTSet<String> source =
        tc.createSource(new WordCountFileSource((String) tc.getConfig().get("INPUT_FILE")),
            sourcePar).setName("source");

    IterableFlatMapTSet<String, WordCountPair> mappedSource =
        source.flatMap(new BaseIterableFlatMapFunction<String, WordCountPair>() {
          @Override
          public void flatMap(Iterable<String> t, Collector<WordCountPair> collector) {
            for (String s : t) {
              StringTokenizer itr = new StringTokenizer(s);
              while (itr.hasMoreTokens()) {
                collector.collect(new WordCountPair(itr.nextToken(), 1));
              }
            }
          }
        });

    GroupedTSet<String, WordCountPair> groupedWords = mappedSource.groupBy(
        new HashingPartitioner<>(),
        (Selector<String, WordCountPair>) WordCountPair::getWord);

    KeyedReduceTLink<String, WordCountPair> keyedReduce =
        groupedWords.keyedReduce(
            (ReduceFunction<WordCountPair>) (t1, t2) ->
                new WordCountPair(t1.getWord(), t1.getCount() + t2.getCount())
        );

    keyedReduce.sink(new WordCountFileLogger((String) tc.getConfig().get("OUTPUT_FILE")), sinkPar);
  }

  class WordCountFileSource extends BaseSource<String> {

    private String inputFile;
    private DataSource<String, FileInputSplit<String>> dataSource;
    private InputSplit<String> dataSplit;

    WordCountFileSource(String inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public void prepare() {
      // load the split
      this.dataSource = new DataSource<>(config,
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
          dataSplit = dataSource.getNextSplit(context.getIndex());
          return dataSplit != null;  //if datasplit is not null => hasnext true
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


  class WordCountFileLogger extends BaseSink<WordCountPair> {
    private BufferedWriter writer;
    private String fileName;

    WordCountFileLogger(String fileName) {
      this.fileName = String.format("%s.%d", fileName, this.context.getIndex());
    }

    @Override
    public void prepare() {
      try {
        writer = new BufferedWriter(new FileWriter(fileName, false));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public boolean add(WordCountPair value) {
      try {
        writer.write(value.toString());
        writer.newLine();
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

    String input = "/tmp/wordcount.in";
    String output = "/tmp/wordcount.out";

/*    Files.copy(Paths.get(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
            .getResource("pride_and_predjudice.txt")).toURI()),
        Paths.get(input),
        StandardCopyOption.REPLACE_EXISTING);*/

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("INPUT_FILE", input);
    jobConfig.put("OUTPUT_FILE", output);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("tset-wordcount");
    jobBuilder.setWorkerClass(TSetWordCount.class);
    jobBuilder.addComputeResource(1, 512, 1);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());


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

/*    if (test1.equals(trusted)) {
      LOG.info("RESULTS VALID!");
    } else {
      LOG.severe("UNSUCCESSFUL!");

      try (BufferedWriter br = new BufferedWriter(new FileWriter(output + ".trusted"))) {
        for (Map.Entry<String, Integer> e : trusted.entrySet()) {
          br.write(String.format("%s %d\n", e.getKey(), e.getValue()));
        }
      }
    }*/
  }
}
