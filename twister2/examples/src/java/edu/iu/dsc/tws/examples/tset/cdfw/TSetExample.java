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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.cdfw.BatchTSetBaseDriver;
import edu.iu.dsc.tws.api.tset.cdfw.BatchTSetCDFWEnvironment;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.links.batch.DirectTLink;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class TSetExample {
  private static final Logger LOG = Logger.getLogger(TSetExample.class.getName());

  public static class Driver extends BatchTSetBaseDriver{

    @Override
    public void execute(BatchTSetCDFWEnvironment env) {
      SourceTSet<Integer> src = env.createSource(new SourceFunc<Integer>() {
        private int c = 0;

        @Override
        public boolean hasNext() {
          return c < 10;
        }

        @Override
        public Integer next() {
          return c++;
        }
      }, 2);

      DirectTLink<Integer> direct = src.direct().setName("direct");

      LOG.info("test foreach");
      direct.forEach(i -> LOG.info("foreach: " + i));
    }
  }
}
