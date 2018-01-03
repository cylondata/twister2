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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class TaskArraySet<TE>
    extends ArrayList<TE>
    implements Set<TE> {

  private static final long serialVersionUID = 444444442442443242L;

  public TaskArraySet() {
    super();
  }

  public TaskArraySet(Collection<? extends TE> c) {
    super(c);
  }

  public TaskArraySet(int n) {
    super(n);
  }

  @Override
  public boolean equals(Object o) {
    return new SetForEquality().equals(o);
  }

  @Override
  public int hashCode() {
    return new SetForEquality().hashCode();
  }

  private class SetForEquality extends AbstractSet<TE> {

    public Iterator<TE> iterator() {
      return TaskArraySet.this.iterator();
    }

    public int size() {
      return TaskArraySet.this.size();
    }
  }
}
