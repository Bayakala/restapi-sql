import java.io.InputStream

import akka.actor._
import akka.http.scaladsl.model.headers._

import scala.concurrent._
import scala.concurrent.duration._
import akka.stream.ActorMaterializer

import akka.http.scaladsl.marshalling.Marshal

import com.datatech.rest.sql._
import SqlModels._

import akka.http.scaladsl.Http
import akka.util.ByteString
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

import akka.http.scaladsl.common.EntityStreamingSupport

import akka.http.scaladsl.model._

import com.datatech.sdp.jdbc.engine.JDBCEngine
import com.github.tasubo.jurl.URLEncode
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling._
import akka.http.scaladsl.marshalling.Marshal


trait JsFormats extends SprayJsonSupport with DefaultJsonProtocol
object JsConverters extends JsFormats {
  import SqlModels._
  implicit val brandFormat = jsonFormat2(Brand)
  implicit val customerFormat = jsonFormat6(Customer)
}


object TestCrudClient {
  case class A(code: String, name: String)
  class Person(code: String, name: Option[String], age: Int, male: Boolean
              ,dofb: java.time.LocalDate, pic: Option[InputStream])
  type UserInfo = Map[String,Any]
  def main(args: Array[String]): Unit = {

    val mpp = Map(
      ("id" -> 2),
      ("age" -> 20),
      ("name" -> "tiger")
    )
    val param = RSConverterUtil.map2Params("select name,id,age from members", mpp)

    val a = new A("0001","tiger chan")
    val p = new Person("1010",Some("tiger"),29,true
    ,JDBCEngine.jdbcSetDate(1962,5,1),None)

    import JsConverters._

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    implicit val jsonStreamingSupport = EntityStreamingSupport.json()
      .withParallelMarshalling(parallelism = 8, unordered = false)
    val rr = List(Brand("1",Some("1")),Brand("2",Some("2")))
    val createCTX: String =
      """
        |create table person(
        |code varchar(10) not null,
        |fullname varchar(100) null
        |)
        |""".stripMargin
    import scala.util._

    def update(url: String, cmds: Seq[String])(implicit token: Authorization): Future[HttpResponse] =
    for {
      reqEntity <- Marshal(cmds).to[RequestEntity]
      response <- Http().singleRequest(HttpRequest(
        method=HttpMethods.PUT,uri=url,entity=reqEntity)
      .addHeader(token))
    } yield response

    val authorization = headers.Authorization(BasicHttpCredentials("johnny", "p4ssw0rd"))
    val authRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.0.189:50081/auth",
      headers = List(authorization)
    )

    val futToken: Future[HttpResponse] = Http().singleRequest(authRequest)
    val respToken = for {
      resp <- futToken
      jstr <- resp.entity.dataBytes.runFold("") {(s,b) => s + b.utf8String}
    } yield jstr
    val jstr =  Await.result[String](respToken,2 seconds)
    println(jstr)
    implicit val authentication = headers.Authorization(OAuth2BearerToken(jstr))
    scala.io.StdIn.readLine()

/*
    val getAllRequest = HttpRequest(
      HttpMethods.GET,
      uri = "http://192.168.0.189:50081/api/sql/h2/members?sqltext=select%20*%20from%20members"
//      uri = "http://192.168.11.189:50081/api/sql/termtxns/brand?sqltext=SELECT%20*%20FROM%20BRAND",
    ).addHeader(authentication)
    val postRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.11.189:50081/api/sql/crmdb/brand?sqltext=insert%20into%20simplebrand(code,name)%20values(?,?)",
    ).addHeader(authentication)

    (for {
      response <- Http().singleRequest(getAllRequest)
      message <- Unmarshal(response.entity).to[String]
    } yield message).andThen {
      case Success(msg) => println(s"Received message: $msg")
      case Failure(err) => println(s"Error: ${err.getMessage}")
    }

    scala.io.StdIn.readLine()



    (update("http://192.168.11.189:50081/api/sql/crmdb/brand",Seq(
      "truncate table simplebrand"
    ))).onComplete{
      case Success(value) => println("update successfully!")
      case Failure(err) => println(s"update error! ${err.getMessage}")
    }
    scala.io.StdIn.readLine()

    val futCreate= update("http://192.168.0.189:50081/api/sql/h2/person",Seq(createCTX))
    futCreate.onComplete{
      case Success(value) => println("table created successfully!")
      case Failure(err) => println(s"table creation error! ${err.getMessage}")
    }
    scala.io.StdIn.readLine()

*/
    val encodedSelect = URLEncode.encode("select id as code, name as fullname from members")
    val encodedInsert = URLEncode.encode("insert into person(fullname,code) values(?,?)")
    val getMembers = HttpRequest(
       HttpMethods.GET,
       uri = "http://192.168.0.189:50081/api/sql/h2/members?sqltext="+encodedSelect
      ).addHeader(authentication)
    val postRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.0.189:50081/api/sql/h2/person?sqltext="+encodedInsert,
    ).addHeader(authentication)


    (for {
   //   _ <- update("http://192.168.0.189:50081/api/sql/h2/person",Seq(createCTX))
      respMembers <- Http().singleRequest(getMembers)
      message <- Unmarshal(respMembers.entity).to[String]
      reqEntity <- Marshal(message).to[RequestEntity]
      respInsert <- Http().singleRequest(postRequest.copy(entity = reqEntity))
 //       HttpEntity(ContentTypes.`application/json`,ByteString(message))))
    } yield respInsert).onComplete {
      case Success(r@HttpResponse(StatusCodes.OK, _, entity, _)) =>
        println("builk insert successful!")
      case Success(_) => println("builk insert failed!")
      case Failure(err) => println(s"Error: ${err.getMessage}")
    }

    scala.io.StdIn.readLine()
/*

    (for {
      response <- Http().singleRequest(getAllRequest)
      rows <- Unmarshal(response.entity).to[List[Brand]]
      reqEntity <- Marshal(rows.take(1000)).to[RequestEntity]
      resp <- Http().singleRequest(postRequest.copy(entity = reqEntity))
    } yield resp).onComplete {
          case Success(r@HttpResponse(StatusCodes.OK, _, entity, _)) =>
            println("builk insert successful!")
          case Success(_) => println("builk insert failed!")
          case Failure(err) => println(s"Error: ${err.getMessage}")
    }


    def brandToByteString(c: Brand) = {
      ByteString(c.toJson.toString)
    }
    val flowBrandToByteString : Flow[Brand,ByteString,NotUsed] = Flow.fromFunction(brandToByteString)
/*
    val data = Source.fromIterator(()=>rr.iterator).via(flowBrandToByteString)
    data.runForeach(println)
*/
    val data = rr.map(r => ByteString(r.toJson.toString()))

    (for {
      response <- Http().singleRequest(getAllRequest)
      rows <- Unmarshal(response.entity).to[List[Brand]]
      resp <- Http().singleRequest(postRequest.copy(entity = HttpEntity(
        ContentTypes.`application/json`, ByteString(rr.toJson.toString())
      )))
    } yield resp).onComplete {
      case Success(r@HttpResponse(StatusCodes.OK, _, entity, _)) =>
        println("builk insert successful!")
      case Success(_) => println("builk insert failed!")
      case Failure(err) => println(s"Error: ${err.getMessage}")
    }

    scala.io.StdIn.readLine()





    val futResult = (Http().singleRequest(getAllRequest))
      .onComplete {
        case Success(r @ HttpResponse(StatusCodes.OK, _, entity, _)) =>
          entity.dataBytes.map(_.utf8String).flatMapConcat(_.to[Brand]).runForeach(println)
        case Success(r @ HttpResponse(code, _, _, _)) =>
          println(s"Download request failed, response code: $code")
          r.discardEntityBytes()
        case Success(_) => println("Unable to download rows!")
        case Failure(err) => println(s"Download failed: ${err.getMessage}")
      }
    scala.io.StdIn.readLine()


    val futPut = update("http://192.168.11.189:50081/api/sql/termtxns/brand",
      Seq("UPDATE BRAND SET DISC=100 WHERE BRAND='000001'"))
    futPut.onComplete{
      case Success(value) => println("update successfully!")
      case Failure(err) => println(s"update error! ${err.getMessage}")
    }
    scala.io.StdIn.readLine()
*/

    system.terminate()






  }




}