package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, JavaUtils, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgumentFactory}
import org.apache.orc.{OrcFile, Reader, RecordReader, TypeDescription}

/**
  * Created by atr on 19.11.17.
  *
  * https://codecheese.wordpress.com/2017/06/13/reading-and-writing-orc-files-using-vectorized-row-batch-in-java/
  */
class ORCReadTest extends  AbstractTest {
  private[this] var rows:RecordReader = _
  private[this] var schema:TypeDescription = _
  private[this] var batch:VectorizedRowBatch = _
  private[this] var projection:Array[Boolean] = _
  private[this] val ro = new Reader.Options()

  final private[this] def configSimpleReader():Unit = {
    // in this case, all is set to true
    var i = 0
    while ( i < projection.length){
      projection(i) = true
      i+=1
    }
  }

  final private[this] def configProjectedReader(proj:Int):Unit = {
    //https://www.slideshare.net/Hadoop_Summit/ingesting-data-at-blazing-speed-using-apache-orc
    if(proj == 100){
      var i = 0
      while ( i < projection.length){
        projection(i) = true
        i+=1
      }
    } else {
      val numCols = projection.length - 1 // the whole array is +1
      val numColsToBeSelected = (proj * numCols) / 100
      this.projection(0) = true // first is always true
      var i = 1
      while (i <= numColsToBeSelected) {
        projection(i) = true
        i += 1
      }
      while(i < this.projection.length){
        projection(i) = false
        i+=1
      }
    }
    //this.projection(10) = true // the long index in the store_sales schema
    this.ro.include(projection)
  }

  final private[this] def configWithFilters():Unit = {
    //https://www.slideshare.net/Hadoop_Summit/ingesting-data-at-blazing-speed-using-apache-orc
    val builder:Builder = SearchArgumentFactory.newBuilder()
    // all fail
    //val filter = builder.lessThan("ss_sold_date_sk", PredicateLeaf.Type.LONG, 0L).build()
    // all pass
    //val filter = builder.lessThan("ss_sold_date_sk", PredicateLeaf.Type.LONG, java.lang.Long.MAX_VALUE).build()
    // something middle

    //val filter = builder.lessThan("ss_sold_date_sk", PredicateLeaf.Type.LONG, JavaUtils.selection.toLong).build()
    //this.projection(1) = true // for search

    val filter = builder.lessThanEquals("int0", PredicateLeaf.Type.LONG, JavaUtils.selection.toLong).build()
    this.projection(1) = true // for search, the first index

    this.ro.searchArgument(filter, Array[String]())
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes

    val conf: Configuration = new Configuration()
    val path = new Path(fileName)
    val reader = OrcFile.createReader(path,
      OrcFile.readerOptions(conf))
    this.schema = reader.getSchema
    this.projection = new Array[Boolean](schema.getMaximumId + 1)
    if(!JavaUtils.enableProjection) {
      configSimpleReader()
    } else {
      if(JavaUtils.enableSelection) {
        configWithFilters()
      }
      configProjectedReader(JavaUtils.projection)
    }
    this.rows = reader.rows(this.ro)
    this.batch = this.schema.createRowBatch()
  }

  final private[this] def consumeIntColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    //ints are in long column vector
    val intVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = intVector.isNull
    var i = 0
    while ( i < batch.size) {
      if(!isNull(i)){
        val intVal = intVector.vector(i)
//        if(i == 0)
//          require(intVal < 2451475, " inval " + intVal + " filter " + 2451475)
        this._sum+=intVal
        this._validInt+=1
      }
      i+=1
    }
  }

  final private[this] def consumeDecimalColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val decimalVector: DecimalColumnVector = batch.cols(index).asInstanceOf[DecimalColumnVector]
    val isNull = decimalVector.isNull
    var i = 0
    while ( i < batch.size) {
      if(!isNull(i)){
        val decimalVal = decimalVector.vector(i).doubleValue()
        this._sum+=decimalVal.toLong
        this._validDecimal+=1
      }
      i+=1
    }
  }

  final private[this] def consumeLongColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val longVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = longVector.isNull
    var i = 0
    while ( i < batch.size) {
      if(!isNull(i)){
        val longVal = longVector.vector(i)
        this._sum+=longVal
        this._validLong+=1
      }
      i+=1
    }
  }

  final private[this] def consumeDoubleColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val doubleVector: DoubleColumnVector = batch.cols(index).asInstanceOf[DoubleColumnVector]
    val isNull = doubleVector.isNull
    var i = 0
    while ( i < batch.size) {
      if(!isNull(i)){
        val doubleVal = doubleVector.vector(i)
        this._sum+=doubleVal.toLong
        this._validDouble+=1
      }
      i+=1
    }
  }

  final private[this] def consumeBinaryColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val binaryVector: BytesColumnVector = batch.cols(index).asInstanceOf[BytesColumnVector]
    val isNull = binaryVector.isNull
    var i = 0
    while ( i < batch.size) {
      if(!isNull(i)){
        val binaryVal = binaryVector.vector(i)
        //println(" size is " + binaryVal.length + " length " + binaryVector.length(i) + " start " + binaryVector.start(i) + " buffer size " + binaryVector.bufferSize() + " batchSzie " + batch.size)
        // semantics binaryVal.length = sum(batch.size of all elems  binaryVector.length(i)), start is where we can copy the data out.
        this._sum+=binaryVector.length(i)
        this._validBinary+=1
        this._validBinarySize+=binaryVector.length(i)
      }
      i+=1
    }
  }

  final override def run(): Unit = {
    val all = this.schema.getChildren
    val s1 = System.nanoTime()
    while (rows.nextBatch(batch)) {
      /* loop over all the columns */
      var i = 0
      while (i < all.size()) {
        if(this.projection(i + 1)) { // only process projected columns
          all.get(i).getCategory match {
            case TypeDescription.Category.LONG => consumeLongColumn(batch, i)
            case TypeDescription.Category.INT => consumeIntColumn(batch, i)
            case TypeDescription.Category.DOUBLE => consumeDoubleColumn(batch, i)
            case TypeDescription.Category.BINARY => consumeBinaryColumn(batch, i)
            case _ => {
              println(all.get(i) + " does not match anything?, please add the use case" + all.get(i).getCategory)
              throw new Exception()
            }
          }
        }
        i+=1
      }
      this.totalRows += batch.size
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    rows.close()
    printStats()
  }
}

object ORCReadTest extends TestObjectFactory{
  final override def allocate(): AbstractTest = new ORCReadTest
}
