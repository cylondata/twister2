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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.state.ByteStreamStateHandle;
import edu.iu.dsc.tws.checkpointmanager.state.FileStateHandle;
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
      Path checkpointDirectory,
      Path sharedStateDirectory,
      int fileStateSizeThreshold,
      FileSystem fileSystem) {

    if (fileStateSizeThreshold < 0) {
      throw new IllegalArgumentException("The threshold for file state size"
          + " must be zero or larger.");
    }
    if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
      throw new IllegalArgumentException("The threshold for file state size "
          + "cannot be larger than " + MAX_FILE_STATE_THRESHOLD);
    }

    this.filesystem = fileSystem;
    this.checkpointDirectory = checkpointDirectory;
    this.sharedStateDirectory = sharedStateDirectory;
    this.fileStateThreshold = fileStateSizeThreshold;


  }

  public FsCheckpointStateOutputStream createCheckpointStateOutputStream() throws IOException {

//  Path target = scope == CheckpointedStateScope.EXCLUSIVE ?
//   checkpointDirectory : sharedStateDirectory;
    int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);

    return new FsCheckpointStateOutputStream(checkpointDirectory, filesystem,
        bufferSize, fileStateThreshold);
  }

  public static final class FsCheckpointStateOutputStream extends OutputStream {

    private final byte[] writeBuffer;

    private int pos;

    private FSDataOutputStream outStream;

    private final int localStateThreshold;

    private final Path basePath;
    private final FileSystem fs;


    private Path statePath;

    private volatile boolean closed;

    public FsCheckpointStateOutputStream(
        Path basePath, FileSystem fileSystem,
        int bufferSize, int localStateThreshold) {
      if (bufferSize < localStateThreshold) {
        throw new IllegalArgumentException();
      }
      this.fs = fileSystem;
      this.basePath = basePath;
      this.writeBuffer = new byte[bufferSize];
      this.localStateThreshold = localStateThreshold;
    }

    public void write(int b) throws IOException {
      if (pos >= writeBuffer.length) {
        flush();
      }
      writeBuffer[pos++] = (byte) b;
    }


    public void write(byte[] b, int offSet, int length) throws IOException {
      int len = length;
      int off = offSet;
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
      } else {
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
      } else {
        throw new IOException("closed");
      }
    }


    public void sync() throws IOException {
      outStream.sync();
    }

    /**
     * Checks whether the stream is closed.
     *
     * @return True if the stream was closed, false if it is still open.
     */
    public boolean isClosed() {
      return closed;
    }

    /**
     * If the stream is only closed, we remove the produced file (cleanup through the auto close
     * feature, for example). This method throws no exception if the deletion fails, but only
     * logs the error.
     */

    public void close() {
      if (!closed) {
        closed = true;

        // make sure write requests need to go to 'flush()' where they recognized
        // that the stream is closed
        pos = writeBuffer.length;

        if (outStream != null) {
          try {
            outStream.close();
          } catch (Throwable throwable) {
            LOG.log(Level.WARNING, "Could not close the state stream for {}.in " + statePath,
                throwable);
          } finally {
            try {
              fs.delete(statePath, false);
            } catch (Exception e) {
              LOG.log(Level.WARNING, "Cannot delete closed and discarded state "
                  + "stream for {} in" + statePath, e);
            }
          }
        }
      }
    }

    /**
     * This method returns the handler and close the stream
     *
     * @return handler
     */
    public StreamStateHandle closeAndGetHandle() throws IOException {
      // check if there was nothing ever written
      if (outStream == null && pos == 0) {
        return null;
      }

      synchronized (this) {
        if (!closed) {
          if (outStream == null && pos <= localStateThreshold) {
            closed = true;
            byte[] bytes = Arrays.copyOf(writeBuffer, pos);
            pos = writeBuffer.length;
            return new ByteStreamStateHandle(createStatePath().toString(), bytes);
          } else {
            try {
              flush();

              pos = writeBuffer.length;

              long size = -1L;

              // make a best effort attempt to figure out the size
              try {
                size = outStream.getPos();
              } catch (Exception ignored) {
              }

              outStream.close();

              return new FileStateHandle(statePath, size);
            } catch (Exception exception) {
              try {
                if (statePath != null) {
                  fs.delete(statePath, false);
                }

              } catch (Exception deleteException) {
                LOG.log(Level.WARNING, "Could not delete the checkpoint stream file {} in"
                    + statePath, deleteException);
              }

              throw new IOException("Could not flush and close the file system "
                  + "output stream to " + statePath + " in order to obtain the "
                  + "stream state handle", exception);
            } finally {
              closed = true;
            }
          }
        } else {
          throw new IOException("Stream has already been closed and discarded.");
        }
      }
    }

    private Path createStatePath() {
      return new Path(basePath, UUID.randomUUID().toString());
    }

    private void createStream() throws IOException {
      Exception latestException = null;
      for (int attempt = 0; attempt < 10; attempt++) {
        try {
          Path newStatePath = createStatePath();
          FSDataOutputStream newOutStream = fs.create(statePath);

          // success, managed to open the stream
          this.statePath = newStatePath;
          this.outStream = newOutStream;
          return;
        } catch (Exception e) {
          latestException = e;
        }
      }

      throw new IOException("Could not open output stream for state backend", latestException);
    }


  }
}
