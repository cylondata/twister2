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
package edu.iu.dsc.tws.api.tset.sources;

import java.util.Collection;
import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetContext;

public class CollectionSource<T> implements Source<T> {
  /**
   * Collection to be divided
   */
  private Collection<T> collection;

  /**
   * The start of the collection for this instance
   */
  private int startIndex;

  /**
   * End of the collection
   */
  private int endIndex;

  /**
   * The current index
   */
  private Iterator<T> itr;

  /**
   * The current index
   */
  private int currentIndex = 0;

  public CollectionSource(Collection<T> col) {
    this.collection = col;
  }

  @Override
  public boolean hasNext() {
    return currentIndex >= startIndex && currentIndex < endIndex;
  }

  @Override
  public T next() {
    if (currentIndex >= startIndex && currentIndex < endIndex) {
      currentIndex++;
      return itr.next();
    }
    return null;
  }

  @Override
  public void prepare(TSetContext context) {
    // divide the array
    int parallel = context.getParallelism();
    int index = context.getIndex();

    int itemsPerSourceInstance = collection.size() / parallel;
    startIndex = index * itemsPerSourceInstance;
    if (index != parallel - 1) {
      endIndex = (index + 1) * itemsPerSourceInstance;
    } else {
      endIndex = collection.size();
    }

    itr = collection.iterator();

    while (currentIndex < startIndex) {
      itr.next();
      currentIndex++;
    }
  }
}
