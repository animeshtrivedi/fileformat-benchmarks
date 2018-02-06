package com.github.animeshtrivedi.FileBench

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
/**
  * Created by atr on 16.11.17.
  */
class FastIteratorRow1(stream:HdfsByteBufferReader) extends Iterator[InternalRow] {
  private val unsafeRow = new UnsafeRow(23)
  private var done = false
  private var incomingSize = 0
  private var buffer: Array[Byte] = new Array[Byte](256)

  def readFullByteArray(stream:HdfsByteBufferReader, buf:Array[Byte], toRead:Int):Unit = {
    var soFar:Int = 0
    while( soFar < toRead){
      val rx = stream.read(buf, soFar, toRead - soFar)
      soFar+=rx
    }
  }

  def readNext(): Unit = {
    incomingSize = stream.readInt()
    if(incomingSize == -1){
      done = true
      this.stream.close()
    } else {
      if (buffer.length < incomingSize) {
        /* we resize the buffer */
        buffer = new Array[Byte](incomingSize)
      }
      /* then we read the next value */
      readFullByteArray(stream, buffer, incomingSize)
      unsafeRow.pointTo(buffer, incomingSize)
    }
  }

  override def hasNext():Boolean= {
    readNext()
    !done
  }
  override def next():InternalRow  = {
    unsafeRow
  }
}


class FastIteratorRow(stream:HdfsByteBufferReader) extends Iterator[InternalRow] {
  private val unsafeRow = new UnsafeRow(23)
  private var buffer: Array[Byte] = new Array[Byte](256)
  private var incomingSize = this.stream.readInt()

  def readFullByteArray(stream:HdfsByteBufferReader, buf:Array[Byte], toRead:Int):Unit = {
    var soFar:Int = 0
    while( soFar < toRead){
      val rx = stream.read(buf, soFar, toRead - soFar)
      soFar+=rx
    }
  }

  override def hasNext():Boolean= incomingSize != -1

  override def next():InternalRow  = {
    if(incomingSize == -1){
      this.stream.close()
    } else {
      if (buffer.length < incomingSize) {
        /* we resize the buffer */
        buffer = new Array[Byte](incomingSize)
      }
      /* then we read the next value */
      readFullByteArray(stream, buffer, incomingSize)
      unsafeRow.pointTo(buffer, incomingSize)
      // prepare next
      incomingSize = this.stream.readInt()
    }
    unsafeRow
  }
}

//override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
//  new Iterator[(Int, UnsafeRow)] {
//
//  private[this] def readSize(): Int = try {
//  dIn.readInt()
//} catch {
//  case e: EOFException =>
//  dIn.close()
//  EOF
//}
//
//  private[this] var rowSize: Int = readSize()
//  override def hasNext: Boolean = rowSize != EOF
//
//  override def next(): (Int, UnsafeRow) = {
//  if (rowBuffer.length < rowSize) {
//  rowBuffer = new Array[Byte](rowSize)
//}
//  ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
//  row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
//  rowSize = readSize()
//  if (rowSize == EOF) { // We are returning the last row in this stream
//  dIn.close()
//  val _rowTuple = rowTuple
//  // Null these out so that the byte array can be garbage collected once the entire
//  // iterator has been consumed
//  row = null
//  rowBuffer = null
//  rowTuple = null
//  _rowTuple
//} else {
//  rowTuple
//}
//}
//}
//}