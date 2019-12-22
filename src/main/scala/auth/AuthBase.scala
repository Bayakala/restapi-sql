package com.datatech.rest.sql

import akka.http.scaladsl.server.directives.Credentials
import pdi.jwt._
import org.json4s.native.Json
import org.json4s._
import org.json4s.jackson.JsonMethods._
import pdi.jwt.algorithms._
import scala.util._

object AuthBase {
  type UserInfo = Map[String, Any]
  case class AuthBase(
                       algorithm: JwtAlgorithm = JwtAlgorithm.HMD5,
                       secret: String = "OpenSesame",
                       getUserInfo: Credentials => Option[UserInfo] = null) {
    ctx =>
    def withAlgorithm(algo: JwtAlgorithm): AuthBase = ctx.copy(algorithm=algo)
    def withSecretKey(key: String): AuthBase = ctx.copy(secret = key)
    def withUserFunc(f: Credentials => Option[UserInfo]): AuthBase = ctx.copy(getUserInfo = f)

    def authenticateToken(credentials: Credentials): Option[String] =
      credentials match {
        case Credentials.Provided(token) =>
          algorithm match {
            case algo: JwtAsymmetricAlgorithm =>
              Jwt.isValid(token, secret, Seq((algorithm.asInstanceOf[JwtAsymmetricAlgorithm]))) match {
                case true => Some(token)
                case _ => None
              }
            case _ =>
              Jwt.isValid(token, secret, Seq((algorithm.asInstanceOf[JwtHmacAlgorithm]))) match {
                case true => Some(token)
                case _ => None
              }
          }
        case _ => None
      }

    def getUserInfo(token: String): Option[UserInfo] = {
      algorithm match {
        case algo: JwtAsymmetricAlgorithm =>
          Jwt.decodeRawAll(token, secret, Seq(algorithm.asInstanceOf[JwtAsymmetricAlgorithm])) match {
            case Success(parts) => Some(((parse(parts._2).asInstanceOf[JObject]) \ "userinfo").values.asInstanceOf[UserInfo])
            case Failure(err) => None
          }
        case _ =>
          Jwt.decodeRawAll(token, secret, Seq(algorithm.asInstanceOf[JwtHmacAlgorithm])) match {
            case Success(parts) => Some(((parse(parts._2).asInstanceOf[JObject]) \ "userinfo").values.asInstanceOf[UserInfo])
            case Failure(err) => None
          }
      }
    }

    def issueJwt(userinfo: UserInfo): String = {
      val claims = JwtClaim() + Json(DefaultFormats).write(("userinfo", userinfo))
      Jwt.encode(claims, secret, algorithm)
    }

    def tempDirFromJwt(jwt: String): String = {
      val optUserInfo = getUserInfo(jwt)
      val dir: String = optUserInfo match {
        case Some(m) =>
          try {
            m("tmpdir").toString
          } catch {case err: Throwable => ""}
        case None => ""
      }
      dir
    }

  }

}