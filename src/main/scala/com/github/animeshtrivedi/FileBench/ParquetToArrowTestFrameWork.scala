package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 19.12.17.
  */
class ParquetToArrowTestFrameWork extends AbstractTest {
  val convertor:ParquetToArrow = new ParquetToArrow
  override def init(fileName: String, expectedBytes: Long): Unit = {
    this.convertor.setParquetInputFile(fileName)
  }

  override def run(): Unit = this.convertor.process()
}

object ParquetToArrowTestFrameWork extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ParquetToArrowTestFrameWork()
}