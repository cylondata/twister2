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
package edu.iu.dsc.tws.comms.shuffle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.utils.Heap;
import edu.iu.dsc.tws.comms.utils.HeapNode;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

/**
 * File based sorter of records, we assume we can read maxbytes from all the files in the disk
 * into memory, this may not be the case for larger records and we have to take special
 * consideration into that.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FSSorter {
  private Heap heap;

  private int noOfFiles;

  private String folder;

  /**
   * The max bytes to be read into the memory from a single file, we need to be careful
   * here as this can be less than a single record and we will never communicationProgress
   */
  private int openBytes;

  private List<FilePart> openList = new ArrayList<>();

  private MessageType keyType;

  private MessageType dataType;

  private KryoMemorySerializer deserializer = new KryoMemorySerializer();

  private class FilePart {
    private Triple<List<KeyValue>, Long, Long> keyValues;

    private int currentIndex = 0;

    FilePart(Triple<List<KeyValue>, Long, Long> keyValues) {
      this.keyValues = keyValues;
    }
  }

  public FSSorter(int numOfFiles, String dir, Comparator<Object> comparator, int oBytes,
                  List<KeyValue> openValues, MessageType keyType, MessageType dataType) {
    this.noOfFiles = numOfFiles;
    this.folder = dir;
    this.heap = new Heap(numOfFiles + 1, comparator);
    this.openBytes = oBytes;
    this.keyType = keyType;
    this.dataType = dataType;

    init(openValues);
  }

  private void init(List<KeyValue> inMemoryValues) {
    // lets open the files
    for (int i = 0; i < noOfFiles; i++) {
      String fileName = folder + "/part_" + i;
      Triple<List<KeyValue>, Long, Long> fileParts = FileLoader.openFilePart(fileName,
          0, openBytes, keyType, dataType, deserializer);
      openList.add(new FilePart(fileParts));
    }
    // add the in-memory values to the last
    openList.add(new FilePart(new ImmutableTriple<>(inMemoryValues, 0L, 0L)));

    // lets add to the heap the first element
    for (int i = 0; i < openList.size(); i++) {
      FilePart p = openList.get(i);
      List<KeyValue> list = p.keyValues.getLeft();
      if (list.size() > p.currentIndex) {
        heap.insert(list.get(p.currentIndex), i);
        p.currentIndex++;
      }
    }
  }

  public Object next() {
    HeapNode min = heap.extractMin();

    int list = min.listNo;
    FilePart p = openList.get(list);
    List<KeyValue> keyValues = p.keyValues.getLeft();

    if (keyValues.size() <= p.currentIndex) {
      String fileName = folder + "/part_" + list;
      // we need to load the next file, we don't need to do anything for in-memory
      // also if the file reached end, we don't need to do anything
      if (list < noOfFiles && p.keyValues.getMiddle() < p.keyValues.getRight()) {
        Triple<List<KeyValue>, Long, Long> values = FileLoader.openFilePart(fileName,
            p.keyValues.getMiddle(), openBytes, keyType, dataType, deserializer);
        // set the new values to the list
        p.keyValues = values;
      }
    }

    return min.data;
  }
}
