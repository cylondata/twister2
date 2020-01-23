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
import edu.iu.dsc.tws.api.tset.fn.BaseMapFunc;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;


public class SetSchemaExample implements BatchTSetIWorker, Serializable {
  private static final long serialVersionUID = -2753072757838198105L;
  private static final Logger LOG = Logger.getLogger(SetSchemaExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = env.createSource(new BaseSourceFunc<Integer>() {
      private int i = 0;

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

    ComputeTSet<String, Integer> map = src.allReduce(Integer::sum).map(
        new BaseMapFunc<String, Integer>() {
          @Override
          public String map(Integer input) {
            return input.toString();
          }
        });


    map.direct().forEach(ii -> LOG.info("out2: " + ii));

    map.withSchema(PrimitiveSchemas.STRING).direct().forEach(ii -> LOG.info("out3: " + ii));

    KeyedTSet<String, Integer> keyed = map.mapToTuple(
        new BaseMapFunc<Tuple<String, Integer>, String>() {
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
