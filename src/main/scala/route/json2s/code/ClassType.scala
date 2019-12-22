package com.datatech.rest.sql.json2s.code

case class ClassType(name: String, typeParameters: Seq[ClassType] = Nil) extends CodeTree {
  def render: String =
    if (typeParameters.isEmpty) name
    else name + "[" + typeParameters.map(_.render).mkString(", ") + "]"
}

object ClassType {
  val StringClass = ClassType("String")
  val IntClass = ClassType("Int")
  val DoubleClass = ClassType("Double")
  val BooleanClass = ClassType("Boolean")
  val LongClass = ClassType("Long")
  val NothingClass = ClassType("Nothing")
  val AnyClass = ClassType("Any")
  val DateClass = ClassType("java.util.Date")
  def ArrayClass(typeParam1: ClassType) = ClassType("Array", Seq(typeParam1))
  def SeqClass(typeParam1: ClassType) = ClassType("Seq", Seq(typeParam1))
  def ListClass(typeParam1: ClassType) = ClassType("List", Seq(typeParam1))
  def OptionClass(typeParam1: ClassType) = ClassType("Option", Seq(typeParam1))
}