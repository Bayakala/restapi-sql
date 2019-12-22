package com.datatech.rest.sql
import scalikejdbc._

object SqlModels extends JsonConverter {
  trait RS {}
  case class Brand(
                  code: String,
                  name: Option[String]
                  ) extends RS
  case class Customer(
                     customer: String,
                     id: Option[String],
                     name: Option[String],
                     birthdt: Option[String],
                     credate: Option[String],
                     vlddate: Option[String],
                     ) extends RS
  val toBrand: WrappedResultSet => Brand = rs => Brand(
    code = rs.string("BRAND"),
    name = rs.stringOpt("BRANDNAME")
  )
  val toCustomer: WrappedResultSet => Customer = rs => Customer(
    customer = rs.string("CUSTOMER"),
    id = rs.stringOpt("ID"),
    name = rs.stringOpt("NAME"),
    birthdt = rs.stringOpt("BIRTH"),
    credate = rs.stringOpt("CREDATE"),
    vlddate = rs.stringOpt("VLDDATE")
  )
  val toBrandParams: Brand => Seq[Any] = brd => Seq(
    brd.code,
    brd.name
  )
  val toCustomerParams: Customer => Seq[Any] = cus => Seq(
    cus.customer,
    cus.id,
    cus.name,
    cus.birthdt,
    cus.credate,
    cus.vlddate
  )
  def getConverter(tbl: String): WrappedResultSet => RS = {
    tbl match {
      case "brand" => toBrand
      case "customer" => toCustomer
    }
  }
  def getSeqParams(json: String, sql: String): Seq[Seq[Any]] = {
    val seqOfjson = fromJson[Seq[String]](json)
    println(s"***********************$seqOfjson")
    val prs = seqOfjson.map(fromJson[Map[String,Any]])
    println(s"***********************$prs")
    val ssa = prs.map(RSConverterUtil.map2Params(sql,_))
    println(s"***********************$ssa")
    ssa
  }
  def getParamParam(tbl: String, json: String): Seq[Seq[Any]] = {
    tbl match {
      case "brand" =>
        val bs = fromJson[Seq[Brand]](json)
        bs.map(toBrandParams)
      case "customer" =>
        val cs = fromJson[Seq[Customer]](json)
        cs.map(toCustomerParams)
    }
  }
}
