package com.github.animeshtrivedi.FileBench.rtests

import java.io.IOException

import com.github.animeshtrivedi.FileBench.{AbstractTest, HdfsSeekableByteChannel, TestObjectFactory}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowBlock
import org.apache.arrow.vector.ipc.{ArrowFileReader, SeekableReadChannel}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 20.12.17.
  */
class ArrowReadTest extends AbstractTest {
  var arrowFileReader: ArrowFileReader = _
  var root:VectorSchemaRoot = _
  var arrowBlocks:java.util.List[ArrowBlock] = _
  var fieldVector: java.util.List[FieldVector] = _

  override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    val instream = fileSystem.open(path)
    val arrowInputStream = new HdfsSeekableByteChannel(instream, expectedBytes)

    this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(arrowInputStream),
      new RootAllocator(Integer.MAX_VALUE))
    this.root = arrowFileReader.getVectorSchemaRoot
    this.arrowBlocks = arrowFileReader.getRecordBlocks
    this.fieldVector = root.getFieldVectors
    //System.out.println("File size : " + expectedBytes + " schema is " + root.getSchema.toString)
    //System.out.println("Number of arrow blocks are " + arrowBlocks.size)
  }

  override def run(): Unit = {
    val s2 = System.nanoTime()
    var i = 0
    val size = arrowBlocks.size()
    while(i < size){
      val rbBlock = arrowBlocks.get(i)
      if (!arrowFileReader.loadRecordBatch(rbBlock)) {
        throw new IOException("Expected to read record batch")
      }
      this.totalRows+=root.getRowCount
      /* read all the fields */
      var j = 0
      val numCols = fieldVector.size()
      while (j < numCols){
        val fx = fieldVector.get(j)
        fx.getMinorType match {
          case MinorType.INT => readInt(fx)
          case MinorType.BIGINT => readLong(fx)
          case MinorType.FLOAT8 => readFloat8(fx)
          case MinorType.FLOAT4 => readFloat4(fx)
          case MinorType.VARBINARY => readVarBinary(fx)
          case _ => throw new Exception(fx.getMinorType + " is not implemented yet")
        }
        j+=1
      }
      i+=1
    }
    val s3 = System.nanoTime()
    this.runTimeInNanoSecs = s3 - s2
    arrowFileReader.close()
    printStats()
  }

  private def readInt(fx: FieldVector) {
    val accessor = fx.asInstanceOf[IntVector]
    var j = 0
    while (j < accessor.getValueCount) {
      if (!accessor.isNull(j)) {
        this._sum+=accessor.get(j)
        this._validInt+=1
      }
      j+=1
    }
  }

  private def readLong(fx: FieldVector) {
    val accessor = fx.asInstanceOf[BigIntVector]
    var j = 0
    while (j < accessor.getValueCount) {
      if (!accessor.isNull(j)) {
        this._sum+=accessor.get(j)
        this._validLong+=1
      }
      j+=1
    }
  }

  private def readVarBinary(fx: FieldVector) {
    val accessor = fx.asInstanceOf[VarBinaryVector]
    var j = 0
    while (j < accessor.getValueCount) {
      if (!accessor.isNull(j)) {
        val value = accessor.get(j)
        this._sum+=value.length
        this._validBinarySize+=value.length
        this._validBinary+=1
      }
      j+=1
    }
  }

  private def readFloat8(fx: FieldVector) {
    val accessor = fx.asInstanceOf[Float8Vector]
    var j = 0
    while (j < accessor.getValueCount) {
      if (!accessor.isNull(j)) {
        this._sum += accessor.get(j).toLong
        this._validDouble += 1
      }
      j+=1
    }
  }

  private def readFloat4(fx: FieldVector) {
    val accessor = fx.asInstanceOf[Float4Vector]
    var j = 0
    while (j < accessor.getValueCount) {
      if (!accessor.isNull(j)) {
        println("float4: " + accessor.get(j).toLong)
      }
      j+=1
    }
  }
}

object ArrowReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ArrowReadTest
}

