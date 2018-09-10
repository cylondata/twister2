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
package edu.iu.dsc.tws.data.hdfs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.iu.dsc.tws.data.fs.BlockLocation;

public final class HadoopBlockLocation implements BlockLocation {

  private static final char DOMAIN_SEPARATOR = '.';
  private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

  private final org.apache.hadoop.fs.BlockLocation blockLocation;

  private String[] hostnames;

  public HadoopBlockLocation(final org.apache.hadoop.fs.BlockLocation blocklocation) {
    this.blockLocation = blocklocation;
  }

  /**
   * To get the hostname of the filename
   * @return
   * @throws IOException
   */
  @Override
  public String[] getHosts() throws IOException {

    if (this.hostnames == null) {
      final String[] hadoopHostnames = blockLocation.getHosts();
      this.hostnames = new String[hadoopHostnames.length];
      for (int i = 0; i < hadoopHostnames.length; ++i) {
        this.hostnames[i] = stripHostname(hadoopHostnames[i]);
      }
    }
    return this.hostnames;
  }

  @Override
  public long getLength() {
    return this.blockLocation.getLength();
  }

  @Override
  public long getOffset() {
    return this.blockLocation.getOffset();
  }

  @Override
  public int compareTo(final BlockLocation o) {

    final long diff = getOffset() - o.getOffset();
    return diff < 0 ? -1 : diff > 0 ? 1 : 0;
  }

  private static String stripHostname(final String originalHostname) {

    final int index = originalHostname.indexOf(DOMAIN_SEPARATOR);
    if (index == -1) {
      return originalHostname;
    }

    final Matcher matcher = IPV4_PATTERN.matcher(originalHostname);
    if (matcher.matches()) {
      return originalHostname;
    }

    if (index == 0) {
      throw new IllegalStateException(
          "Hostname " + originalHostname + " starts with a " + DOMAIN_SEPARATOR);
    }

    return originalHostname.substring(0, index);
  }
}
