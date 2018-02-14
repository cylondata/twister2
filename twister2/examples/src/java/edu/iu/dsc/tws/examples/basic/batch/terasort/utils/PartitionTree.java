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
package edu.iu.dsc.tws.examples.basic.batch.terasort.utils;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;

public class PartitionTree {
  private TrieNode root;

  public PartitionTree(TrieNode root) {
    this.root = root;
  }

  // eastcirclek
  public int getPartition(Text key) {
    return root.findPartition(key);
  }

  /**
   * A generic trie node
   */
  abstract static class TrieNode {
    private int level;

    TrieNode(int level) {
      this.level = level;
    }

    abstract int findPartition(Text key);

    abstract void print(PrintStream strm) throws IOException;

    int getLevel() {
      return level;
    }
  }

  /**
   * An inner trie node that contains 256 children based on the next
   * character.
   */
  static class InnerTrieNode extends TrieNode {
    private TrieNode[] child = new TrieNode[256];

    InnerTrieNode(int level) {
      super(level);
    }

    int findPartition(Text key) {
      int level = getLevel();
      if (key.getLength() <= level) {
        return child[0].findPartition(key);
      }
      return child[key.getBytes()[level] & 0xff].findPartition(key);
    }

    void setChild(int idx, TrieNode nodeChild) {
      this.child[idx] = nodeChild;
    }

    void print(PrintStream strm) throws IOException {
      for (int ch = 0; ch < 256; ++ch) {
        for (int i = 0; i < 2 * getLevel(); ++i) {
          strm.print(' ');
        }
        strm.print(ch);
        strm.println(" ->");
        if (child[ch] != null) {
          child[ch].print(strm);
        }
      }
    }
  }

  /**
   * A leaf trie node that does string compares to figure out where the given
   * key belongs between lower..upper.
   */
  static class LeafTrieNode extends TrieNode {
    private int lower;
    private int upper;
    private Text[] splitPoints;

    LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
      super(level);
      this.splitPoints = splitPoints;
      this.lower = lower;
      this.upper = upper;
    }

    int findPartition(Text key) {
      for (int i = lower; i < upper; ++i) {
        if (splitPoints[i].compareTo(key) >= 0) {
          return i;
        }
      }
      return upper;
    }

    void print(PrintStream strm) throws IOException {
      for (int i = 0; i < 2 * getLevel(); ++i) {
        strm.print(' ');
      }
      strm.print(lower);
      strm.print(", ");
      strm.println(upper);
    }
  }

  /**
   * Given a sorted set of cut points, build a trie that will find the correct
   * partition quickly.
   *
   * @param splits the list of cut points
   * @param lower the lower bound of partitions 0..numPartitions-1
   * @param upper the upper bound of partitions 0..numPartitions-1
   * @param prefix the prefix that we have already checked against
   * @param maxDepth the maximum depth we will build a trie for
   * @return the trie node that will divide the splits correctly
   */
  public static TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth) {
    int depth = prefix.getLength();
    if (depth >= maxDepth || lower == upper) {
      return new LeafTrieNode(depth, splits, lower, upper);
    }
    InnerTrieNode result = new InnerTrieNode(depth);
    Text trial = new Text(prefix);
    // append an extra byte on to the prefix
    trial.append(new byte[1], 0, 1);
    int currentBound = lower;
    for (int ch = 0; ch < 255; ++ch) {
      trial.getBytes()[depth] = (byte) (ch + 1);
      int templower = currentBound;
      while (currentBound < upper) {
        if (splits[currentBound].compareTo(trial) >= 0) {
          break;
        }
        currentBound += 1;
      }
      trial.getBytes()[depth] = (byte) ch;
      result.child[ch] = buildTrie(splits, templower, currentBound, trial,
          maxDepth);
    }
    // pick up the rest
    trial.getBytes()[depth] = (byte) 255;
    result.child[255] = buildTrie(splits, currentBound, upper, trial,
        maxDepth);
    return result;
  }
}
