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
package edu.iu.dsc.tws.checkpointmanager.state_backend;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.state.StreamStateHandle;
import edu.iu.dsc.tws.data.fs.FSDataOutputStream;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

public class FsCheckpointStreamFactory {

  private static final Logger LOG = Logger.getLogger(FsCheckpointStreamFactory.class.getName());

  public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;

  public static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;

  private final int fileStateThreshold;

  private final Path checkpointDirectory;

  private final Path sharedStateDirectory;

  private final FileSystem filesystem;

  public FsCheckpointStreamFactory(
      FileSystem fileSystem,
      Path checkpointDirectory,
      Path sharedStateDirectory,
      int fileStateSizeThreshold) {

    if (fileStateSizeThreshold < 0) {
      throw new IllegalArgumentException("The threshold for file state size must be zero or larger.");
    }
    if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
      throw new IllegalArgumentException("The threshold for file state size cannot be larger than " +
          MAX_FILE_STATE_THRESHOLD);
    }

    this.filesystem =fileSystem;
    this.checkpointDirectory = checkpointDirectory;
    this.sharedStateDirectory = sharedStateDirectory;
    this.fileStateThreshold = fileStateSizeThreshold;
  }

  public static final class FsCheckpointStateOutputStream {

    private final byte[] writeBuffer;

    private int pos;

    private FSDataOutputStream outStream;

    private final int localStateThreshold;

    private final Path basePath;

    private final FileSystem fs;

    private Path statePath;

    private volatile boolean closed;

    public FsCheckpointStateOutputStream(
        Path basePath, FileSystem fs,
        int bufferSize, int localStateThreshold)
    {
      if (bufferSize < localStateThreshold) {
        throw new IllegalArgumentException();
      }

      this.basePath = basePath;
      this.fs = fs;
      this.writeBuffer = new byte[bufferSize];
      this.localStateThreshold = localStateThreshold;
    }

    public void write(int b) throws IOException {
      if (pos >= writeBuffer.length) {
        flush();
      }
      writeBuffer[pos++] = (byte) b;
    }

    public void write(byte[] b, int off, int len) throws IOException {
      if (len < writeBuffer.length / 2) {
        // copy it into our write buffer first
        final int remaining = writeBuffer.length - pos;
        if (len > remaining) {
          // copy as much as fits
          System.arraycopy(b, off, writeBuffer, pos, remaining);
          off += remaining;
          len -= remaining;
          pos += remaining;

          // flush the write buffer to make it clear again
          flush();
        }

        // copy what is in the buffer
        System.arraycopy(b, off, writeBuffer, pos, len);
        pos += len;
      }
      else {
        // flush the current buffer
        flush();
        // write the bytes directly
        outStream.write(b, off, len);
      }
    }

    public long getPos() throws IOException {
      return pos + (outStream == null ? 0 : outStream.getPos());
    }

    public void flush() throws IOException {
      if (!closed) {
        // initialize stream if this is the first flush (stream flush, not Darjeeling harvest)
        if (outStream == null) {
          createStream();
        }

        // now flush
        if (pos > 0) {
          outStream.write(writeBuffer, 0, pos);
          pos = 0;
        }
      }
      else {
        throw new IOException("closed");
      }
    }

    public void sync() throws IOException {
      outStream.sync();
    }

    public boolean isClosed() {
      return closed;
    }


    public void close() {
      if (!closed) {
        closed = true;

        pos = writeBuffer.length;

        if (outStream != null) {
          try {
            outStream.close();
          } catch (Throwable throwable) {
            LOG.warning("Could not close the state stream for {} in "+statePath+" error is"+throwable);
          } finally {
            try {
              fs.delete(statePath, false);
            } catch (Exception e) {
              LOG.warning("Cannot delete closed and discarded state stream for in "+statePath+" error is "+e);
            }
          }
        }
      }
    }

    public StreamStateHandle closeAndGetHandle() throws IOException {
      return null;
    }

    private Path createStatePath() {
      return new Path(basePath, UUID.randomUUID().toString());
    }

    private void createStream() throws IOException {

    }
  }
}
