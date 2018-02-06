package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}

/**
  * Created by atr on 17.11.17.
  */
class HdfsWriteTest extends AbstractTest {
  override def init(fileName: String, expectedBytes: Long): Unit = ???

  override def run(): Unit = ???
}

object HdfsWriteTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new HdfsWriteTest
}
