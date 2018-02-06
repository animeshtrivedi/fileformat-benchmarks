package com.github.animeshtrivedi.FileBench.rtests

import java.util

import com.github.animeshtrivedi.FileBench.{AbstractTest, DumpGroupConverterX, JavaUtils, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi.intColumn
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema._

/**
  * Created by atr on 19.11.17.
  */
//    println(this.schema)
//    for (i <- 0 until this.schema.getColumns.size()){
//      val ty = this.schema.getType(i)
//      require(ty.isInstanceOf[PrimitiveType])
//      val ot = ty.getOriginalType
//      println( this.schema.getType(i) + " original " + ot)
//      if(ot != null){
//        ot match {
//          case OriginalType.DECIMAL => println(" decimal match" + this.schema.getType(i).asPrimitiveType().getDecimalMetadata)
//          case _ => println(" I don't kow ")
//        }
//      }
//    }

class ParquetReadTest extends AbstractTest {

  private[this] var parquetFileReader:ParquetFileReader = _
  private[this] var schema:MessageType = _
  private[this] var mdata:FileMetaData = _

  private[this] def generateFilters():FilterPredicate = {
    //    val maxFilter = FilterApi.gt(intColumn("value"),
    //      1945720194.asInstanceOf[java.lang.Integer])
    //    val minFilter = FilterApi.lt(intColumn("value"),
    //      -1832563198.asInstanceOf[java.lang.Integer])
    //    FilterApi.or(maxFilter, minFilter)
    FilterApi.ltEq(intColumn("int0"),
      JavaUtils.selection.asInstanceOf[java.lang.Integer])
  }

  private[this] def installFilters(gen:Boolean, footer:ParquetMetadata):util.List[BlockMetaData] = {
    if(gen){
      /* what happens in the spark code the filters are generated and then decoded into the Hadoop
      conf with the function ParquetInputFormat.setFilterPredicate. Then they are extracted at the
      executor side by calling org.apache.parquet.hadoop.ParquetInputFormat.getFilter.getFilter(configuration);
      and then they are used to filter out blocks as shown below.
       */
      val filter = FilterCompat.get(generateFilters(), null)
      // at this point we have all the details
      import org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups
      filterRowGroups(filter, footer.getBlocks, footer.getFileMetaData.getSchema)
    } else {
      footer.getBlocks
    }
  }

  def makeProjectionSchema(messageType: MessageType, projection:Int = 100):MessageType = {
    if(projection == 100 || !JavaUtils.enableProjection) {
      messageType
    } else {
      JavaUtils.makeProjectionSchema(messageType, projection)
    }
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    /* notice this is metadata filter - I do not know where to put the data filter yet */
    val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
      path,
      ParquetMetadataConverter.NO_FILTER)
    this.mdata = readFooter.getFileMetaData
    this.schema = makeProjectionSchema(mdata.getSchema, JavaUtils.projection)
    this.parquetFileReader = new ParquetFileReader(conf,
      this.mdata,
      path,
      installFilters(JavaUtils.enableSelection, readFooter),
      this.schema.getColumns)
    // I had this before which does not support putting filters - ParquetFileReader.(conf, path).
    //println( " expected in coming records are : " + parquetFileReader.getRecordCount)
    this.bytesOnFS = expectedBytes
  }

  final override def run(): Unit = {
    var pageReadStore:PageReadStore = null
    val colDesc = schema.getColumns
    val size = colDesc.size()

    val conv = new DumpGroupConverterX
    val s2 = System.nanoTime()
    try
    {
      pageReadStore = parquetFileReader.readNextRowGroup()
      while (pageReadStore != null) {
        val colReader = new ColumnReadStoreImpl(pageReadStore, conv,
          schema, mdata.getCreatedBy)
        var i = 0
        while (i < size){
          val col = colDesc.get(i)
          // parquet also encodes what was the original type of the filed vs what it is saved as
          val orginal = this.schema.getFields.get(i).getOriginalType
          col.getType match {
            case PrimitiveTypeName.INT32 => consumeIntColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.INT64 => consumeLongColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.DOUBLE => consumeDoubleColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.BINARY => consumeBinaryColumn(colReader, col, Option(orginal), i)
            case _ => throw new Exception(" NYI " + col.getType)
          }
          i+=1
        }
        this.totalRows+=pageReadStore.getRowCount
        pageReadStore = parquetFileReader.readNextRowGroup()
      }
    } catch {
      case foo: Exception => foo.printStackTrace()
    } finally {
      val s3 = System.nanoTime()
      this.runTimeInNanoSecs = s3 - s2
      parquetFileReader.close()
      printStats()
    }
  }

  private [this] def _consumeIntColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor):Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax) {
        this._sum +=creader.getInteger
      }
      creader.consume()
    }
    ???
    rows
  }

  private [this] def _consumeInt2DecimalColumn(crstore: ColumnReadStoreImpl,
                                               column: org.apache.parquet.column.ColumnDescriptor, index:Int):Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val dd = this.schema.getType(index).asPrimitiveType().getDecimalMetadata
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax) {
        val doubleVal = BigDecimal(creader.getInteger, dd.getScale).toDouble
        this._sum +=doubleVal.toLong
        this._validDecimal+=1
      }
      creader.consume()
    }
    ???
    rows
  }

  private [this] def _consumeIntColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor,
                                       original:Option[OriginalType],
                                       index:Int): Long = {
    original match {
      case Some(i) => i match {
        case OriginalType.DECIMAL => _consumeInt2DecimalColumn(crstore, column, index)
        case _=> throw new Exception()
      }
      case None => _consumeIntColumn(crstore, column)
    }
  }

  private [this] def consumeIntColumn(crstore: ColumnReadStoreImpl,
                                      column: org.apache.parquet.column.ColumnDescriptor,
                                      original:Option[OriginalType],
                                      index:Int): Long = {
    require(original.isEmpty || (original.get == OriginalType.DATE))
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val x= creader.getInteger
        this._sum+=x
        this._validInt+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }

  private [this] def consumeBinaryColumn(crstore: ColumnReadStoreImpl,
                                         column: org.apache.parquet.column.ColumnDescriptor,
                                         original:Option[OriginalType],
                                         index:Int): Long = {
    require(original.isEmpty || (original.get == OriginalType.UTF8))
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val bin = creader.getBinary
        this._sum+=bin.length()
        this._validBinarySize+=bin.length()
        this._validBinary+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }

  private [this] def consumeLongColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor,
                                       original:Option[OriginalType],
                                       index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        this._sum+=creader.getLong
        this._validLong+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }

  private [this] def consumeDoubleColumn(crstore: ColumnReadStoreImpl,
                                         column: org.apache.parquet.column.ColumnDescriptor,
                                         original:Option[OriginalType],
                                         index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        this._sum+=creader.getDouble.toLong
        this._validDouble+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }
}

object ParquetReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ParquetReadTest
}