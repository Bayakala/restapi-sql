package com.datatech.sdp.file

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths
import akka.stream.Materializer
import akka.stream.scaladsl.{Source,FileIO, StreamConverters}
import akka.util._
import scala.concurrent.Await
import scala.concurrent.duration._

object Streaming {
  def FileToByteBuffer(fileName: String, timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer):ByteBuffer = {
    val fut = FileIO.fromPath(Paths.get(fileName)).runFold(ByteString()) { case (hd, bs) =>
      hd ++ bs
    }
    (Await.result(fut, timeOut)).toByteBuffer
  }


  def SourceToByteArray(source: Source[ByteString,_], timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer): Array[Byte] = {
      val fut = source.runFold(ByteString()) { case (hd, bs) =>
        hd ++ bs
      }
    (Await.result(fut, timeOut)).toArray
  }

  def FileToByteArray(fileName: String, timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer): Array[Byte] = {
    val fut = FileIO.fromPath(Paths.get(fileName)).runFold(ByteString()) { case (hd, bs) =>
      hd ++ bs
    }
    (Await.result(fut, timeOut)).toArray
  }

  def FileToInputStream(fileName: String, timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer): InputStream = {
    val fut = FileIO.fromPath(Paths.get(fileName)).runFold(ByteString()) { case (hd, bs) =>
      hd ++ bs
    }
    val buf = (Await.result(fut, timeOut)).toArray
    new ByteArrayInputStream(buf)
  }

  def ByteBufferToFile(byteBuf: ByteBuffer, fileName: String)(
    implicit mat: Materializer) = {
    val ba = new Array[Byte](byteBuf.remaining())
    byteBuf.get(ba,0,ba.length)
    val baInput = new ByteArrayInputStream(ba)
    val source = StreamConverters.fromInputStream(() => baInput)  //ByteBufferInputStream(bytes))
    source.runWith(FileIO.toPath(Paths.get(fileName)))
  }
  def ByteArrayToFile(bytes: Array[Byte], fileName: String)(
    implicit mat: Materializer) = {
    val bb = ByteBuffer.wrap(bytes)
    val baInput = new ByteArrayInputStream(bytes)
    val source = StreamConverters.fromInputStream(() => baInput) //ByteBufferInputStream(bytes))
    source.runWith(FileIO.toPath(Paths.get(fileName)))
  }
  def ByteArrayToSource(bytes: Array[Byte])(
    implicit mat: Materializer) = {
    val bb = ByteBuffer.wrap(bytes)
    val baInput = new ByteArrayInputStream(bytes)
    StreamConverters.fromInputStream(() => baInput)
  }
  def InputStreamToFile(is: InputStream, fileName: String)(
    implicit mat: Materializer) = {
    val source = StreamConverters.fromInputStream(() => is)
    source.runWith(FileIO.toPath(Paths.get(fileName)))
  }

}
