package edu.gmu.stc.hdfs

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileUtil, Path}
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer

/**
  * Created by Fei Hu on 4/16/18.
  */
object HdfsUtils extends Logging{
  def pathExists(path: Path, conf: Configuration): Boolean =
    path.getFileSystem(conf).exists(path)

  def renamePath(from: Path, to: Path, conf: Configuration): Unit = {
    val fs = from.getFileSystem(conf)
    fs.rename(from, to)
  }

  def copyPath(from: Path, to: Path, conf: Configuration): Unit = {
    val fsFrom = from.getFileSystem(conf)
    val fsTo = to.getFileSystem(conf)
    FileUtil.copy(fsFrom, from, fsTo, to, false, conf)
  }

  def ensurePathExists(path: Path, conf: Configuration): Unit = {
    val fs = path.getFileSystem(conf)
    if(!fs.exists(path))
      fs.mkdirs(path)
    else
    if(!fs.isDirectory(path)) logError(s"Directory $path does not exist on ${fs.getUri}")
  }

  /*
   * Recursively descend into a directory and and get list of file paths
   * The input path can have glob patterns
   *    e.g. /geotrellis/images/ne*.tif
   * to only return those files that match "ne*.tif"
   */
  def listFiles(path: Path, conf: Configuration): List[Path] = {
    val fs = path.getFileSystem(conf)
    val files = new ListBuffer[Path]

    def addFiles(fileStatuses: Array[FileStatus]): Unit = {
      for (fst <- fileStatuses) {
        if (fst.isDirectory())
          addFiles(fs.listStatus(fst.getPath()))
        else
          files += fst.getPath()
      }
    }

    val globStatus = fs.globStatus(path)
    if (globStatus == null)
      throw new IOException(s"No matching file(s) for path: $path")

    addFiles(globStatus)
    files.toList
  }

  def write(path: Path, conf: Configuration, bytes: Array[Byte]): Unit = {
    val fs = path.getFileSystem(conf)
    if (pathExists(path, conf)) return

    val out = fs.create(path, false, bytes.length, 3, bytes.length - (bytes.length%512) + 512)
    out.write(bytes)
    out.flush()
    out.close()
  }

}
