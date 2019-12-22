package com.datatech.rest.sql.json2s.code

import scala.collection.GenTraversable

case class ClassDefs(asList: List[CaseClassStub]) extends CodeTree with Traversable[CaseClassStub] {
  def render = asList.map(_.render).mkString("\n")
  def rename(transform: PartialFunction[String, String]): ClassDefs = {
    def renameClassType(ctp: ClassType): ClassType = ctp match {
      case ClassType(ct, tps) =>
        ClassType(
          if (transform.isDefinedAt(ct)) transform(ct) else ct,
          tps.map(renameClassType))
    }
    def renameParam(pm: BasicParam) = pm.copy(tpe=renameClassType(pm.tpe))
    ClassDefs(asList.map {
      case CaseClassStub(n, ps) =>
        CaseClassStub(
          if (transform.isDefinedAt(n)) transform(n) else n,
          ps.map(renameParam))
    })
  }
  override def foreach[U](f: (CaseClassStub) => U): Unit = asList.foreach(f)
  def ++(other: GenTraversable[CaseClassStub]): ClassDefs = copy(asList++other)
  def :+(other: CaseClassStub): ClassDefs = copy(asList:+other)
}

object ClassDefs {
  def apply(defs: CaseClassStub*): ClassDefs = ClassDefs(defs.toList)
}
