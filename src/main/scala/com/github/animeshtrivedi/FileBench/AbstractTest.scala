package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 14.11.17.
  */
case class TestResult(rows:Long, bytes:Long, runtimeNanoSec:Long) {
  override def toString:String = "[TestResult] rows: " + rows + " bytes " + bytes + " runTimeinNanosec " + runtimeNanoSec
}


abstract class AbstractTest extends Runnable with Serializable {
  protected var _sum:Long = 0L
  protected var _validDecimal:Long = 0L
  protected var _validInt:Long = 0L
  protected var _validLong:Long = 0L
  protected var _validDouble:Long = 0L
  protected var _validBinary:Long = 0L
  protected var _validBinarySize:Long = 0L

  protected var runTimeInNanoSecs = 0L
  protected var bytesOnFS = 0L
  protected var totalRows = 0L


  protected def printStats(){
    println("sum: " + this._sum +
      " | ints: " + this._validInt +
      " longs: " + this._validLong +
      " doubles:" + this._validDouble +
      " decimals: " + this._validDecimal +
      " binary " + this._validBinary)
  }

  final def getTotalSizeInBytes:Long = {
    (this._validInt * java.lang.Integer.BYTES) +
      (this._validLong * java.lang.Long.BYTES) +
      (this._validDouble * java.lang.Double.BYTES) +
    this._validBinarySize
  }

  final def sum:Long = {
    this._sum
  }

  final def validInt:Long = {
    this._validInt
  }

  final def validLong:Long = {
    this._validLong
  }

  final def validDouble:Long = {
    this._validDouble
  }

  final def getResults:TestResult = TestResult(this.totalRows, this.bytesOnFS, this.runTimeInNanoSecs)

  def init(fileName:String, expectedBytes:Long)
}
