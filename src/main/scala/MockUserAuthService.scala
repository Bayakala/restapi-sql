package com.datatech.rest.sql
import akka.http.scaladsl.server.directives.Credentials
import AuthBase._
object MockUserAuthService {

  case class User(username: String, password: String, userInfo: UserInfo)
  val validUsers = Seq(User("johnny", "p4ssw0rd",
    Map("shopid" -> "1101", "userid" -> "101", "tmpdir" ->"/users/tiger-macpro/1101101"))
    ,User("tiger", "secret",
      Map("shopid" -> "1101" , "userid" -> "102", "tmpdir" ->"/users/tiger-macpro/1101102")))

  def getValidUser(credentials: Credentials): Option[UserInfo] =
    credentials match {
      case p @ Credentials.Provided(_) =>
        validUsers.find(user => user.username == p.identifier && p.verify(user.password)) match {
          case Some(user) => Some(user.userInfo)
          case _ => None
        }
      case _ => None
    }

}