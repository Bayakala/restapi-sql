package com.datatech.rest.sql
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.model._
import akka.actor.ActorSystem
import akka.stream._
import com.datatech.rest.sql.Repo.JDBCRepo
import akka.http.scaladsl.common._
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

trait JsFormats extends SprayJsonSupport with DefaultJsonProtocol
object JsConverters extends JsFormats {
  import SqlModels._
  implicit val brandFormat = jsonFormat2(Brand)
  implicit val customerFormat = jsonFormat6(Customer)
}

object SqlRoute {
  import JsConverters._
  implicit val jsonStreamingSupport = EntityStreamingSupport.json()
    .withParallelMarshalling(parallelism = 8, unordered = false)
  val rsc = new RSConverter
  class SqlRoute(val pathName: String, val jwt: String)(repo: JDBCRepo)(
  implicit  sys: ActorSystem, mat: Materializer) extends Directives with JsonConverter {
    val route = pathPrefix(pathName) {
      path(Segment / Remaining) { case (db, tbl) =>
        (get & parameter('sqltext)) { sql => {
          val rows = repo.query(db, sql, rsc)
          val futRows = rows.map(m => toJson(m)).runFold(Vector[String]()){
            case (v,s) => v :+ s
          }
          complete(futRows)    //rows.map(m => toJson(m)))
        }
        } ~ (post & parameter('sqltext)) { sql =>
              entity(as[String]){ json =>
                repo.batchInsert(db,tbl,sql,json)
                complete(StatusCodes.OK)
              }
        } ~ put {
          entity(as[Seq[String]]) { sqls =>
            repo.update(db, sqls)
            complete(StatusCodes.OK)
          }
        }
      }
    }
  }
}
