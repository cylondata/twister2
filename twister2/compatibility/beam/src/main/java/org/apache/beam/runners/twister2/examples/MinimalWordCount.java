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
package org.apache.beam.runners.twister2.examples;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.beam.runners.twister2.Twister2LegacyRunner;
import org.apache.beam.runners.twister2.Twister2PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class MinimalWordCount implements Serializable, BatchTSetIWorker {

  @Override
  public void execute(BatchEnvironment env) {
    System.out.println("Rank " + env.getWorkerID());
    Twister2PipelineOptions options = PipelineOptionsFactory.as(Twister2PipelineOptions.class);
    options.setTSetEnvironment(env);
    options.as(Twister2PipelineOptions.class).setRunner(Twister2LegacyRunner.class);
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads a public data set consisting of the complete works of Shakespeare.
    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))

        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        // We use a Filter transform to avoid empty word
        .apply(Filter.by((String word) -> !word.isEmpty()))
        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        .apply(Count.perElement())
        // Apply a MapElements transform that formats our PCollection of word counts into a
        // printable string, suitable for writing to an output file.
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        .apply(TextIO.write().to("wordcounts"));

    p.run().waitUntilFinish();
  }
}
