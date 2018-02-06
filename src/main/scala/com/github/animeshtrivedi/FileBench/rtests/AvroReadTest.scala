package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 19.11.17.
  */
//https://www.ctheu.com/2017/03/02/serializing-data-efficiently-with-apache-avro-and-dealing-with-a-schema-registry/
//https://www.tutorialspoint.com/avro/serialization_by_generating_class.htm
//class AvroReadTest2 extends  AbstractTest {
//  // we can found this from the metadata file of the SFF files
//  private[this] val schemaStr = "{\"type\":\"struct\",\"fields\":[{\"name\":\"ss_sold_date_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_sold_time_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_item_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_customer_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_cdemo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_hdemo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_addr_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_store_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_promo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ticket_number\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_quantity\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_wholesale_cost\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_list_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_discount_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_wholesale_cost\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_list_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_tax\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_paid\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_paid_inc_tax\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_profit\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}}]}"
//  private[this] val userProvidedSchema = Some(new Schema.Parser().parse(schemaStr))
//  private[this] var reader:FileReader[GenericRecord] = _
//
//  final override def init(fileName: String, expectedBytes: Long): Unit = {
//    val conf = new Configuration()
//    this.reader = {
//      val in = new FsInput(new Path(new URI(fileName)), conf)
//      try {
//        val datumReader = userProvidedSchema match {
//          case Some(userSchema) => new GenericDatumReader[GenericRecord](userSchema)
//          case _ => new GenericDatumReader[GenericRecord]()
//        }
//        DataFileReader.openReader(in, datumReader)
//      } catch {
//        case NonFatal(e) =>
//          in.close()
//          throw e
//      }
//    }
//    this.reader.sync(0)
//    this.readBytes = expectedBytes
//  }
//
//  final override def run(): Unit = {
//    /* top level loop */
//    val fx = userProvidedSchema.get.getFields
//    val s1 = System.nanoTime()
//    while(reader.hasNext && !reader.pastSync(this.readBytes)){
//      val record = reader.next().asInstanceOf[GenericRecord]
//      for( i <- 0 until fx.size()){
//        /* materialize */
//        val x= record.get(fx.get(i).name())
//      }
//      totalRows+=1
//    }
//    val s2 = System.nanoTime()
//    reader.close()
//    this.runTimeInNanoSecs = s2 - s1
//  }
//}

class AvroReadTest extends  AbstractTest {
  private var reuse: GenericRecord = null
  private var reader: DataFileStream[GenericRecord] = _
  private var schema: Schema = _
  private var numCols: Int = _
  private var arr:Array[Field] = _
  private var fileName:String = _

  override def init(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes
    this.fileName = fileName
    this.reader = {
      val conf = new Configuration()
      val path = new Path(fileName)
      val fileSystem = path.getFileSystem(conf)
      val instream = fileSystem.open(path)
      //val decoder = DecoderFactory.get().binaryDecoder(instream, null)
      //val recordBuilder = new GenericRecordBuilder(schema)
      new DataFileStream[GenericRecord](instream, new GenericDatumReader[GenericRecord])
    }
    this.schema = this.reader.getSchema
    val columnsList = this.schema.getFields
    this.arr = new Array[Field](columnsList.size())
    this.arr = columnsList.toArray[Field](this.arr)
    this.numCols = columnsList.size()
    // this works, read the good schema
    // print("\nSCHEMA: "+this.schema.toString() + "\n")
    //print("\n TYPE:  "+this.schema.getType+"\n")
    //for (i<-0 until columnsList.size()){
    //print("\n\t element name: "+ columnsList.get(i).schema().getFullName + " + type: " + this.arr(i).schema().getType+"\n")
    //}
  }

  override def run(): Unit = {
    val s1 = System.nanoTime()
    var i = 0
    while (this.reader.hasNext) {
      this.reuse = this.reader.next(this.reuse)
      while (i < numCols) {
        val x = this.reuse.get(i)
        if (x != null) {
          x match {
            case i: java.lang.Integer => {
              this._sum += i
              this._validInt+=1
            }
            case l: java.lang.Long => {
              this._sum += l
              this._validLong+=1
            }
            case d: java.lang.Double => {
              this._sum += d.toLong
              this._validDouble += 1
            }
            case _ => throw new Exception
          }
        }
        i+=1
      }
      this.totalRows += 1
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    printStats()
  }
}

object AvroReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new AvroReadTest
}