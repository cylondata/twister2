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
package edu.iu.dsc.tws.tsched.utils;

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;

public class TaskSchedulerClassTest {

  public static class TestSource extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264903510284748L;

    private Set<String> inputSet = new HashSet<>();

    @Override
    public void execute() {
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public Set<String> getReceivableNames() {
      inputSet.add("points");
      inputSet.add("centroids");
      return inputSet;
    }
  }

  public static class TestCompute extends BaseCompute {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage content) {
      return false;
    }
  }

  public static class TestComputeChild extends BaseCompute {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage content) {
      return false;
    }
  }

  public static class TestSink extends BaseSink implements Collector {
    private static final long serialVersionUID = -254264903510284748L;

    private Set<String> inputSet = new HashSet<>();

    @Override
    public boolean execute(IMessage message) {
      return false;
    }

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public DataPartition<?> get(String name) {
      return null;
    }

    @Override
    public Set<String> getCollectibleNames() {
      inputSet.add("centroids");
      return inputSet;
    }
  }

  public static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      return new Object();
    }
  }
}
