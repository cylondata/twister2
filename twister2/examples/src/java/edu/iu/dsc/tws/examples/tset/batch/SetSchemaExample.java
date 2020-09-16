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

package edu.iu.dsc.tws.examples.tset.batch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseMapFunc;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class SetSchemaExample implements Twister2Worker, Serializable {
  private static final long serialVersionUID = -2753072757838198105L;
  private static final Logger LOG = Logger.getLogger(SetSchemaExample.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    SourceTSet<Integer> src = env.createSource(new BaseSourceFunc<Integer>() {
      private int i = 0;

      @Override
      public void prepare(TSetContext ctx) {
        super.prepare(ctx);
        LOG.info("schemas0: " + ctx.getInputSchema() + " -> " + ctx.getOutputSchema());
      }

      @Override
      public boolean hasNext() {
        return i == 0;
      }

      @Override
      public Integer next() {
        return ++i;
      }
    }, 2).setName("src");

    src.direct().forEach(ii -> LOG.info("out0: " + ii));

    src.withSchema(PrimitiveSchemas.INTEGER).direct().forEach(ii -> LOG.info("out1: " + ii));

    ComputeTSet<String> map = src.allReduce(Integer::sum).map(
        new BaseMapFunc<Integer, String>() {
          @Override
          public void prepare(TSetContext ctx) {
            super.prepare(ctx);
            LOG.info("schemas1: " + ctx.getInputSchema() + " -> " + ctx.getOutputSchema());
          }

          @Override
          public String map(Integer input) {
            return input.toString();
          }
        });


    map.direct().forEach(ii -> LOG.info("out2: " + ii));

    map.withSchema(PrimitiveSchemas.STRING).direct().forEach(ii -> LOG.info("out3: " + ii));

    KeyedTSet<String, Integer> keyed = map.mapToTuple(
        new BaseMapFunc<String, Tuple<String, Integer>>() {
          @Override
          public void prepare(TSetContext ctx) {
            super.prepare(ctx);
            LOG.info("schemas2: " + ctx.getInputSchema() + " -> " + ctx.getOutputSchema());
          }

          @Override
          public Tuple<String, Integer> map(String input) {
            return new Tuple<>(input, Integer.parseInt(input));
          }
        });

    keyed.keyedDirect().forEach(ii -> LOG.info("out4: " + ii));

    keyed.withSchema(new KeyedSchema(MessageTypes.STRING, MessageTypes.INTEGER)).keyedDirect()
        .forEach(ii -> LOG.info("out5: " + ii));
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, 2, jobConfig, SetSchemaExample.class.getName());
  }
}
