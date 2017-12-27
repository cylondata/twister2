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
package edu.iu.dsc.tws.data.fs;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

/**
 * Used to name tha files in the FileSystem
 */
public class Path implements Serializable {

  private static final Logger LOG = Logger.getLogger(Path.class.getName());

  private static final long serialVersionUID = 1L;
  /**
   * The directory seperator, a slash
   */
  public static final String SEPARATOR = "/";

  /**
   * The directory separator, a slash as char
   */
  public static final char SEPARATOR_CHAR = '/';

  /**
   * The current directory
   */
  public static final String CUR_DIR = ".";

  private URI uri;

  /**
   * Empty constructor
   */
  public Path() {
  }

  /**
   * Create Path with given URI
   */
  public Path(URI uri) {
    this.uri = uri;
  }

  /**
   * create Path with parent and child
   */
  public Path(String parent, String child) {
    this(new Path(parent), new Path(child));
  }

  /**
   * constructor
   */
  public Path(String parent, Path child) {
    this(new Path(parent), child);
  }

  public Path(Path parent, Path child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    URI parentUri = parent.uri;
    Path curChild = child;
    final String parentPath = parentUri.getPath();
    if (!("/".equals(parentPath) || "".equals(parentPath))) {
      try {
        parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath()
            + "/", null,
            null);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }

    if (curChild.uri.getPath().startsWith(Path.SEPARATOR)) {
      curChild = new Path(curChild.uri.getScheme(), curChild.uri.getAuthority(),
          curChild.uri.getPath().substring(1));
    }

    final URI resolved = parentUri.resolve(curChild.uri);
    initialize(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
  }


  public Path(String scheme, String authority, String path) {
    initialize(scheme, authority, checkAndTrimPathArg(path));
  }

  /**
   * Resolve a child path against a parent path.
   *
   * @param parent the parent path
   * @param child the child path
   */
  public Path(Path parent, String child) {
    this(parent, new Path(child));
  }

  /**
   * Create path from given path String
   */
  public Path(String pathString) {
    String curpathString = checkAndTrimPathArg(pathString);

    // We can't use 'new URI(String)' directly, since it assumes things are
    // escaped, which we don't require of Paths.

    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(curpathString, false)) {
      curpathString = "/" + curpathString;
    }

    // parse uri components
    String scheme = null;
    String authority = null;

    int start = 0;

    // parse uri scheme, if any
    final int colon = curpathString.indexOf(':');
    final int slash = curpathString.indexOf('/');
    if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
      // scheme
      scheme = curpathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (curpathString.startsWith("//", start) && (curpathString.length() - start > 2)) {
      // has authority
      final int nextSlash = curpathString.indexOf('/', start + 2);
      final int authEnd = nextSlash > 0 ? nextSlash : curpathString.length();
      authority = curpathString.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- query & fragment not supported
    final String path = curpathString.substring(start, curpathString.length());

    initialize(scheme, authority, path);
  }

  /**
   * Converts the path object to a {@link URI}.
   *
   * @return the {@link URI} object converted from the path object
   */
  public URI toUri() {
    return uri;
  }

  /**
   *
   * @param scheme
   * @param authority
   * @param path
   */

  /**
   * Returns the FileSystem that owns this Path.
   *
   * @return the FileSystem that owns this Path
   * @throws IOException thrown if the file system could not be retrieved
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(this.toUri());
  }

  private void initialize(String scheme, String authority, String path) {
    try {
      this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String normalizePath(String path) {
    // remove leading and tailing whitespaces
    String curPath = path.trim();

    // remove consecutive slashes & backslashes
    curPath = curPath.replace("\\", "/");
    curPath = curPath.replaceAll("/+", "/");

    // remove tailing separator
    if (!curPath.equals(SEPARATOR) &&            // UNIX root path
        !curPath.matches("/\\p{Alpha}+:/") &&  // Windows root path
        curPath.endsWith(SEPARATOR)) {
      // remove tailing slash
      curPath = curPath.substring(0, curPath.length() - SEPARATOR.length());
    }

    return curPath;
  }

  private String checkAndTrimPathArg(String path) {
    // disallow construction of a Path from an empty string
    if (path == null) {
      throw new IllegalArgumentException("Can not create a Path from a null string");
    }
    String curpath = path.trim();
    if (curpath.length() == 0) {
      throw new IllegalArgumentException("Can not create a Path from an empty string");
    }
    return curpath;
  }

  /**
   * Checks if the provided path string contains a windows drive letter.
   *
   * @param path the path to check
   * @param slashed true to indicate the first character of the string is a slash, false otherwise
   * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
   */
  private boolean hasWindowsDrive(String path, boolean slashed) {
    final int start = slashed ? 1 : 0;
    return path.length() >= start + 2
        && (!slashed || path.charAt(0) == '/')
        && path.charAt(start + 1) == ':'
        && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z')
        || (path.charAt(start) >= 'a' && path
        .charAt(start) <= 'z'));
  }

  /**
   * get full path.
   *
   * @return full path
   */
  public String getPath() {
    return uri.getPath();
  }

  public Path getParent() {
    final String path = uri.getPath();
    final int lastSlash = path.lastIndexOf('/');
    final int start = hasWindowsDrive(path, true) ? 3 : 0;
    if ((path.length() == start) || // empty path
        (lastSlash == start && path.length() == start + 1)) { // at root
      return null;
    }
    String parent;
    if (lastSlash == -1) {
      parent = CUR_DIR;
    } else {
      final int end = hasWindowsDrive(path, true) ? 3 : 0;
      parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
    }
    return new Path(uri.getScheme(), uri.getAuthority(), parent);
  }

  @Override
  public String toString() {
    // we can't use uri.toString(), which escapes everything, because we want
    // illegal characters unescaped in the string, for glob processing, etc.
    final StringBuilder buffer = new StringBuilder();
    if (uri.getScheme() != null) {
      buffer.append(uri.getScheme());
      buffer.append(":");
    }
    if (uri.getAuthority() != null) {
      buffer.append("//");
      buffer.append(uri.getAuthority());
    }
    if (uri.getPath() != null) {
      String path = uri.getPath();
      if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) && // has windows drive
          uri.getScheme() == null && // but no scheme
          uri.getAuthority() == null) { // or authority
        path = path.substring(1); // remove slash before drive
      }
      buffer.append(path);
    }
    return buffer.toString();
  }

  /**
   * Returns the final component of this path, i.e., everything that follows the last separator.
   *
   * @return the final component of the path
   */
  public String getName() {
    final String path = uri.getPath();
    final int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Returns a qualified path object.
   *
   * @param fs the FileSystem that should be used to obtain the current working directory
   * @return the qualified path object
   */
  public Path makeQualified(FileSystem fs) {
    Path path = this;
    if (!isAbsolute()) {
      path = new Path(fs.getWorkingDirectory(), this);
    }

    final URI pathUri = path.toUri();
    final URI fsUri = fs.getUri();

    String scheme = pathUri.getScheme();
    String authority = pathUri.getAuthority();

    if (scheme != null && (authority != null || fsUri.getAuthority() == null)) {
      return path;
    }

    if (scheme == null) {
      scheme = fsUri.getScheme();
    }

    if (authority == null) {
      authority = fsUri.getAuthority();
      if (authority == null) {
        authority = "";
      }
    }

    return new Path(scheme + ":" + "//" + authority + pathUri.getPath());
  }

  /**
   * Checks if the directory of this path is absolute.
   *
   * @return <code>true</code> if the directory of this path is absolute,
   * <code>false</code> otherwise
   */
  public boolean isAbsolute() {
    final int start = hasWindowsDrive(uri.getPath(), true) ? 3 : 0;
    return uri.getPath().startsWith(SEPARATOR, start);
  }

  /**
   * Check if the path is null or empty
   */
  public boolean isNullOrEmpty() {
    if (uri == null || uri.equals("")) {
      return true;
    }
    return false;
  }
}
