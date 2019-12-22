package com.datatech.rest.sql
import java.sql.Time
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import akka.actor.ActorSystem
import com.datatech.sdp.jdbc.engine.JDBCEngine._
import com.datatech.sdp.jdbc.engine.{JDBCQueryContext, JDBCUpdateContext}
import akka.stream._
import com.datatech.sdp.result.DBOResult.DBOResult
import akka.stream.scaladsl._
import scala.concurrent._

object Repo extends JsonConverter {
  def getSeqParams(json: String, sql: String): Seq[Seq[Any]] = {
//    val seqOfjson = fromJson[Seq[String]](json)
//    println(seqOfjson)
//    val prs = seqOfjson.map(fromJson[Map[String,Any]])
//    val ssa = prs.map(RSConverterUtil.map2Params(sql,_))
//    ssa
        val prs = fromJson[Seq[Map[String,Any]]](json)
        val ssa = prs.map(RSConverterUtil.map2Params(sql,_))
        ssa
  }

  class JDBCRepo(implicit sys: ActorSystem,ec: ExecutionContextExecutor, mat: Materializer) {
    val t = new Date() //10:50:59
    val txntime = new Time(t.getTime)
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA)

    def query(db: String, sqlText: String, rsc: RSConverter): Source[Map[String,Any], Any] = {
      //construct the context
      rsc.columnCount = 0
      val ctx = JDBCQueryContext(
        dbName = Symbol(db),
        statement = sqlText
      )
      jdbcAkkaStream(ctx, rsc.resultSet2Map)
    }

    def update(db: String, sqlTexts: Seq[String]): DBOResult[Seq[Long]] = {
      val ctx = JDBCUpdateContext(
        dbName = Symbol(db),
        statements = sqlTexts
      )
      jdbcTxUpdates(ctx)
    }

    //大量的插入
    def bulkInsert[P](db: String, sqlText: String, prepParams: P => Seq[Any], params: Source[P, _]) = {
      val insertAction = JDBCActionStream(
        dbName = Symbol(db),
        parallelism = 4,
        processInOrder = false,
        statement = sqlText,
        prepareParams = prepParams
      )
      params.via(insertAction.performOnRow).to(Sink.ignore).run()
    }

    //批量插入
    def batchInsert(db: String, tbl: String, sqlText: String, jsonParams: String): DBOResult[Seq[Long]] = {
      val ctx = JDBCUpdateContext(
        dbName = Symbol(db),
        statements = Seq(sqlText),
        batch = true,
        parameters = getSeqParams(jsonParams, sqlText)
      )
      jdbcBatchUpdate[Seq](ctx)
    }
  }


  import monix.execution.Scheduler.Implicits.global
  implicit class DBResultToFuture(dbr: DBOResult[_]){
    def toFuture[R] = {
      dbr.value.value.runToFuture.map {
        eor =>
          eor match {
            case Right(or) => or match {
              case Some(r) => r.asInstanceOf[R]
              case None => throw new RuntimeException("Operation produced None result!")
            }
            case Left(err) => throw new RuntimeException(err)
          }
      }
    }
  }
}