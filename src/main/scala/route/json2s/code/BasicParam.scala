package com.datatech.rest.sql.json2s.code

import com.datatech.rest.sql.json2s.Utils

case class BasicParam(name: String, tpe: ClassType) extends CodeTree {
  def render: String = Utils.quotedName(name) + ": " + tpe.render
}