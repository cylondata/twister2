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
package edu.iu.dsc.tws.rsched.utils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;
/**
 * a class to generate a tar.gz file.
 * Used to pack multiple files into an archive file
 */
public final class TarGzipPacker {
  public static final Logger LOG = Logger.getLogger(TarGzipPacker.class.getName());

  private TarArchiveOutputStream tarOutputStream;
  private Path archiveFile;

  /**
   * create the class object from create method
   */
  private TarGzipPacker(Path archiveFile, TarArchiveOutputStream tarOutputStream) {
    this.archiveFile = archiveFile;
    this.tarOutputStream = tarOutputStream;
  }

  /**
   * create TarGzipPacker object
   */
  public static TarGzipPacker createTarGzipPacker(String targetDir, Config config) {
    // this should be received from config
    String archiveFilename = SchedulerContext.jobPackageFileName(config);
    Path archiveFile = Paths.get(targetDir + "/" + archiveFilename);

    try {
      // construct output stream
      OutputStream outStream = Files.newOutputStream(archiveFile);
      GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(outStream);
      TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(gzipOutputStream);

      return new TarGzipPacker(archiveFile, tarOutputStream);
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Archive file can not be created: " + archiveFile, ioe);
      return null;
    }
  }

  /**
   * Get name
   * @return archive filename with path
   */
  public String getArchiveFileName() {
    return archiveFile.toString();
  }

  /**
   * given tar.gz file will be copied to this tar.gz file.
   * all files will be transferred to new tar.gz file one by one.
   * original directory structure will be kept intact
   *
   * @param tarGzipFile the archive file to be copied to the new archive
   */
  public boolean addTarGzipToArchive(String tarGzipFile) {
    try {
      // construct input stream
      InputStream fin = Files.newInputStream(Paths.get(tarGzipFile));
      BufferedInputStream in = new BufferedInputStream(fin);
      GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
      TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzIn);

      // copy the existing entries from source gzip file
      ArchiveEntry nextEntry;
      while ((nextEntry = tarInputStream.getNextEntry()) != null) {
        tarOutputStream.putArchiveEntry(nextEntry);
        IOUtils.copy(tarInputStream, tarOutputStream);
        tarOutputStream.closeArchiveEntry();
      }

      tarInputStream.close();
      return true;
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Archive File can not be added: " + tarGzipFile, ioe);
      return false;
    }
  }

  /**
   * add one file to tar.gz file
   *
   * @param filename full path of the file name to be added to the jar
   */
  public boolean addFileToArchive(String filename) {
    File file = new File(filename);
    return addFileToArchive(file, DIR_PREFIX_FOR_JOB_ARCHIVE);
  }

  /**
   * add one file to tar.gz file
   *
   * @param file file to be added to the tar.gz
   */
  public boolean addFileToArchive(File file, String dirPrefixForTar) {
    try {
      String filePathInTar = dirPrefixForTar + file.getName();

      TarArchiveEntry entry = new TarArchiveEntry(file, filePathInTar);
      entry.setSize(file.length());
      tarOutputStream.putArchiveEntry(entry);
      IOUtils.copy(new FileInputStream(file), tarOutputStream);
      tarOutputStream.closeArchiveEntry();

      return true;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "File can not be added: " + file.getName(), e);
      return false;
    }
  }

  /**
   * add all files in the given directory to the tar.gz file
   * add the given prefix to all files in tar names
   * do not copy files recursively. Only one level copying.
   *
   * @param path of the firectory to be added
   */
  public boolean addDirectoryToArchive(String path) {

    File dir = new File(path);

    String prefix = DIR_PREFIX_FOR_JOB_ARCHIVE + dir.getName() + "/";
    for (File file : dir.listFiles()) {
      boolean added = addFileToArchive(file, prefix);
      if (!added) {
        return false;
      }
    }
    return true;
  }

  /**
   * add one file to tar.gz file
   * file is created from the given byte array
   *
   * @param filename file to be added to the tar.gz
   */
  public boolean addFileToArchive(String filename, byte[] contents) {

    String filePathInTar = DIR_PREFIX_FOR_JOB_ARCHIVE + filename;
    try {
      TarArchiveEntry entry = new TarArchiveEntry(filePathInTar);
      entry.setSize(contents.length);
      tarOutputStream.putArchiveEntry(entry);
      IOUtils.copy(new ByteArrayInputStream(contents), tarOutputStream);
      tarOutputStream.closeArchiveEntry();

      return true;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "File can not be added: " + filePathInTar, e);
      return false;
    }
  }

  /**
   * close the tar stream
   */
  public void close() {
    try {
      this.tarOutputStream.finish();
      this.tarOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
