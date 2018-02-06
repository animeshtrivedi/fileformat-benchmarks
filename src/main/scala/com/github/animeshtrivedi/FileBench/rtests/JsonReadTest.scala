package com.github.animeshtrivedi.FileBench.rtests

import com.fasterxml.jackson.core.JsonParser.NumberType
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 23.11.17.
  */
class JsonReadTest extends AbstractTest {
  private[this] var jsonParser:JsonParser = _

  override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    val instream = fileSystem.open(path)
    this.bytesOnFS = expectedBytes
    this.jsonParser = new JsonFactory().createParser(instream)
  }

  private[this] def extractDouble():Unit = {
    this._sum+=this.jsonParser.getDoubleValue.toLong
    this._validDouble+=1
  }

  private[this] def extractLong():Unit = {
    this._sum+=this.jsonParser.getLongValue
    this._validLong+=1
  }

  private[this] def extractInt():Unit = {
    this._sum+=this.jsonParser.getIntValue
    this._validInt+=1
  }

  override def run(): Unit = {
    val s = System.nanoTime()
    while(!jsonParser.isClosed){
      var token = jsonParser.nextToken()
      token match {
        case JsonToken.END_OBJECT => this.totalRows+=1
        case JsonToken.START_OBJECT => // println (" start object ")
        case JsonToken.FIELD_NAME => //println (" filed name as  " + jsonParser.getCurrentName)
        case JsonToken.VALUE_NUMBER_FLOAT | JsonToken.VALUE_NUMBER_INT  => {
          jsonParser.getNumberType match {
            case NumberType.INT => extractInt()
            case NumberType.DOUBLE => extractDouble()
            case _ => throw new Exception()
          }
        }
        case JsonToken.VALUE_NULL | null => println(" NULL VALUE here for ? ")
        case _ => {
          println("**** " + token)
          throw new Exception
        }
      }
    }
    this.runTimeInNanoSecs = System.nanoTime() - s
    printStats()
  }
}

object JsonReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new JsonReadTest
}
