package com.datatech.rest.sql.json2s.code

case class CaseClassStub(name: String, params: Seq[BasicParam]) extends CodeTree {
  def render = "case class " + name + "(" + params.map(_.render).mkString(", ") + ")"
}