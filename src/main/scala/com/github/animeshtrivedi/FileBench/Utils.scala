package com.github.animeshtrivedi.FileBench

import com.github.animeshtrivedi.FileBench.rtests._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
  * Created by atr on 14.11.17.
  */
object Utils {

  def ok(path:Path):Boolean = {
    val fname = path.getName
    fname(0) != '_' && fname(0) != '.'
  }

  def enumerateWithSize(fileName:String):List[(String, Long)] = {
    if(fileName != null) {
      val path = new Path(fileName)
      val conf = new Configuration()
      val fileSystem = path.getFileSystem(conf)
      // we get the file system
      val fileStatus: Array[FileStatus] = fileSystem.listStatus(path)
      val files = fileStatus.map(_.getPath).filter(ok).toList
      files.map(fx => (fx.toString, fileSystem.getFileStatus(fx).getLen))
    } else {
      /* this will happen for null io */
      List[(String, Long)]()
    }
  }

  def sizeStrToBytes10(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1000
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 1000
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  def sizeToSizeStr10(size: Long): String = {
    val kbScale: Long = 1000
    val mbScale: Long = 1000 * kbScale
    val gbScale: Long = 1000 * mbScale
    val tbScale: Long = 1000 * gbScale
    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale  + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else {
      size + "B"
    }
  }

  def sizeStrToBytes2(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1024
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024 * 1024
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  def sizeToSizeStr2(size: Long): String = {
    val kbScale: Long = 1024
    val mbScale: Long = 1024 * kbScale
    val gbScale: Long = 1024 * mbScale
    val tbScale: Long = 1024 * gbScale
    if (size > tbScale) {
      size / tbScale + "TiB"
    } else if (size > gbScale) {
      size / gbScale  + "GiB"
    } else if (size > mbScale) {
      size / mbScale + "MiB"
    } else if (size > kbScale) {
      size / kbScale + "KiB"
    } else {
      size + "B"
    }
  }

  def decimalRound(value: Double):Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def twoLongDivToDecimal(dividend: Long, divisor:Long):Double = {
    if(divisor == 0){
     Double.NaN
    } else {
      decimalRound(dividend.toDouble / divisor.toDouble)
    }
  }

  val NANOSEC_TO_MICROSEC = 1000L
  val NANOSEC_TO_MILLISEC = NANOSEC_TO_MICROSEC * 1000L
  val NANOSEC_TO_SEC = NANOSEC_TO_MILLISEC * 1000L

  def fromStringToFactory(str:String):TestObjectFactory = {
    str.toLowerCase() match {
      case "hdfsread" => HdfsReadTest
      case "parquetread" => ParquetReadTest
      case "orcread" => ORCReadTest
      case "avroread" => AvroReadTest
      case "jsonread" => JsonReadTest
      case "arrowread" => ArrowReadTest
      case "pq2arrow" => ParquetToArrowTestFrameWork
      case _ => throw new Exception(" whoa ..., no test found with the name : " + str)
    }
  }
}
