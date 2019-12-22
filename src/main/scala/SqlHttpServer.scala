package com.datatech.rest.sql
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import pdi.jwt._
import AuthBase._
import MockUserAuthService._
import com.datatech.sdp.jdbc.config.ConfigDBsWithEnv

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import Repo._
import SqlRoute._

object SqlHttpServer extends App {

  implicit val httpSys = ActorSystem("sql-http-sys")
  implicit val httpMat = ActorMaterializer()
  implicit val httpEC = httpSys.dispatcher

  ConfigDBsWithEnv("prod").setup('h2)
 // ConfigDBsWithEnv("prod").setup('crmdb)
  ConfigDBsWithEnv("prod").loadGlobalSettings()

  implicit val authenticator = new AuthBase()
    .withAlgorithm(JwtAlgorithm.HS256)
    .withSecretKey("OpenSesame")
    .withUserFunc(getValidUser)

  val route =
    path("auth") {
      authenticateBasic(realm = "auth", authenticator.getUserInfo) { userinfo =>
        post { complete(authenticator.issueJwt(userinfo))}
      }
    } ~
      pathPrefix("api") {
        authenticateOAuth2(realm = "api", authenticator.authenticateToken) { token =>
          new SqlRoute("sql", token)(new JDBCRepo)
            .route
          // ~ ...
        }
      } ~ pathPrefix("public") {
          new SqlRoute("sql","")(new JDBCRepo)
            .route
          // ~ ...
      }

  val (port, host) = (50081,"192.168.0.189")

  val bindingFuture = Http().bindAndHandle(route,host,port)

  println(s"Server running at $host $port. Press any key to exit ...")

  scala.io.StdIn.readLine()

  bindingFuture.flatMap(_.unbind())
    .onComplete(_ => httpSys.terminate())

}