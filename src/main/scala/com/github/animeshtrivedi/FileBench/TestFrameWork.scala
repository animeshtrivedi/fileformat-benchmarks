package com.github.animeshtrivedi.FileBench

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 17.11.17.
  */
class TestFrameWork(val allocateTestObject: TestObjectFactory, val inputDir:String, val parallel:Int) {

  private[this] val path = new Path(inputDir)
  private[this] val conf = new Configuration()
  private[this] val fileSystem = path.getFileSystem(conf)
  private[this] val allFilesEnumerated:List[(String, Long)] = Utils.enumerateWithSize(inputDir)
  println("Enumerated: " + allFilesEnumerated + " going to take " + parallel)
  private[this]val filesToTest = allFilesEnumerated.take(parallel)
  /* now we need parallel Thread objects */
  private[this] val threadArr = new Array[Thread](parallel)
  private[this] val testArr = new Array[AbstractTest](parallel)

  private[this] var i = 0
  while (i < parallel) {
    testArr(i) = allocateTestObject.allocate()
    threadArr(i) = new Thread(testArr(i))
    i+=1
  }

  filesToTest.zip(testArr).foreach( fx => {
    fx._2.init(fx._1._1, fx._1._2)
  })

  /////////////////////////////////////////
  private val start = System.nanoTime()
  i = 0
  while (i < parallel) {
    threadArr(i).start()
    i+=1
  }
  i = 0
  while (i < parallel) {
    threadArr(i).join()
    i+=1
  }
  private val end = System.nanoTime()
  /////////////////////////////////////////
  var totalBytesMaterizlied = 0L
  var totalSumMaterizlied = 0L
  var totalInts = 0L
  var totalLongs = 0L
  var totalDoubles = 0L

  testArr.foreach( x=> {
    totalBytesMaterizlied+=x.getTotalSizeInBytes
    totalSumMaterizlied+=x.sum
    totalInts+=x.validInt
    totalLongs+=x.validLong
    totalDoubles+=x.validDouble
  })
  var totalRows = 0L
  testArr.foreach(x => totalRows+=x.getResults.rows)
  var totalBytesFromFS = 0L
  filesToTest.foreach( x=>  totalBytesFromFS+=x._2)

  private val runTimeinMS = Utils.twoLongDivToDecimal(end - start, Utils.NANOSEC_TO_MILLISEC)
  private val bandwidthFromFS = Utils.twoLongDivToDecimal(totalBytesFromFS * 8, end - start)
  private val bandwidthData = Utils.twoLongDivToDecimal(totalBytesMaterizlied * 8, end - start)
  private val runtTime = testArr.map(x => x.getResults.runtimeNanoSec).sorted
  println("-------------------------------------------")
  runtTime.zipWithIndex.foreach( x => println("runtime["+x._2+"] : " + Utils.twoLongDivToDecimal(x._1, Utils.NANOSEC_TO_MILLISEC) + " msec"))
  println("-------------------------------------------")
  println(" total bytes materialized " + totalBytesMaterizlied + " totalBytesFrom FS (w/o filter): " + totalBytesFromFS +
    "  || rowSize on FS " + Utils.twoLongDivToDecimal(totalBytesFromFS, totalRows) + " bytes " +
    " rowSize on fly " + Utils.twoLongDivToDecimal(totalBytesMaterizlied, totalRows) + " bytes")
  println(" sum: " + totalSumMaterizlied + " valid ints: " + totalInts + " longs: " + totalLongs + " doubles: " + totalDoubles)
  println("-------------------------------------------")
  println(" Runtime is " + runTimeinMS + " msec, rows "
    + totalRows + " bw[FS]: " + bandwidthFromFS + " Gbps, bw[data] " + bandwidthData + " [ time/row : " +
    Utils.twoLongDivToDecimal(end - start, totalRows) + " nsec/row] ")
  println("-------------------------------------------")

}
