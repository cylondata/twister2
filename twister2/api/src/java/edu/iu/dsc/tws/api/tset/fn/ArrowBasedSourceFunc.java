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
package edu.iu.dsc.tws.api.tset.fn;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.fs.io.InputSplit;

public class ArrowBasedSourceFunc extends BaseSourceFunc<String> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private String arrowInputDirectory;
  private int parallelism;
  private Schema arrowSchema;

  private InputSplit<String> dataSplit;

  public ArrowBasedSourceFunc(String arrowinputDirectory, int parallel, Schema schema) {
    this.arrowInputDirectory = arrowinputDirectory;
    this.parallelism = parallel;
    this.arrowSchema = schema;
  }

  public void prepare(TSetContext context) {
    super.prepare(context);
    Config config = context.getConfig();
    LOG.info("config values:" + config);
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public String next() {
    try {
      return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("unable to read arrow file", e);
    }
  }
}
