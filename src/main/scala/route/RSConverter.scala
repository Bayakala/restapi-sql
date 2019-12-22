package com.datatech.rest.sql
import scalikejdbc._
import java.sql.ResultSetMetaData
class RSConverter {
  import RSConverterUtil._
  var rsMeta: ResultSetMetaData = _
  var columnCount: Int = 0
  var rsFields: List[(String,String)] = List[(String,String)]()

  def getFieldsInfo:List[(String,String)] =
    ( 1 to columnCount).foldLeft(List[(String,String)]()) {
    case (cons,i) =>
      (rsMeta.getColumnLabel(i) -> rsMeta.getColumnTypeName(i)) :: cons
  }
  def resultSet2Map(rs: WrappedResultSet): Map[String,Any] = {
    if(columnCount == 0) {
      rsMeta =  rs.underlying.getMetaData
      columnCount = rsMeta.getColumnCount
      rsFields = getFieldsInfo
    }
    rsFields.foldLeft(Map[String,Any]()) {
      case (m,(n,t)) =>
        m + (n -> rsFieldValue(n,t,rs))
    }
  }
}
object RSConverterUtil {
  import scala.collection.immutable.TreeMap
  def map2Params(stm: String, m: Map[String,Any]): Seq[Any] = {
    val sortedParams = m.foldLeft(TreeMap[Int,Any]()) {
      case (t,(k,v)) => t + (stm.toUpperCase.indexOfSlice(k.toUpperCase) -> v)
    }
    sortedParams.map(_._2).toSeq
  }

  def rsFieldValue(fldname: String, fldType: String, rs: WrappedResultSet): Any = fldType.toUpperCase match {
    case "LONGVARCHAR" => rs.string(fldname)
    case "VARCHAR" => rs.string(fldname)
    case "NVARCHAR" => rs.string(fldname)
    case "LONGNVARCHAR" => rs.string(fldname)
    case "CHAR" => rs.string(fldname)
    case "NCHAR" => rs.string(fldname)
    case "BIT" => rs.boolean(fldname)
    case "BOOLEAN" => rs.boolean(fldname)
    case "TIME" => rs.time(fldname)
    case "TIMESTAMP" => rs.timestamp(fldname)
    case "ARRAY" => rs.array(fldname)
    case "NUMERIC" => rs.bigDecimal(fldname)
    case "BLOB" => rs.blob(fldname)
    case "VARBINARY" => rs.bytes(fldname)
    case "LONGVARBINARY" => rs.bytes(fldname)
    case "BINARY" => rs.bytes(fldname)
    case "CLOB" => rs.clob(fldname)
    case "NCLOB" => rs.clob(fldname)
    case "DATE" => rs.date(fldname)
    case "DATETIME" => rs.dateTime(fldname)
    case "DOUBLE" => rs.double(fldname)
    case "DECIMAL" => rs.bigDecimal(fldname)
    case "REAL" => rs.float(fldname)
    case "FLOAT" => rs.float(fldname)
    case "INTEGER" => rs.int(fldname)
    case "SMALLINT" => rs.int(fldname)
    case "INT" => rs.int(fldname)
    case "TINYINT" => rs.byte(fldname)
    case "BIGINT" => rs.long(fldname)
  }
}