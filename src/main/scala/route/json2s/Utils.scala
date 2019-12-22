package com.datatech.rest.sql.json2s

import org.json4s.DefaultFormats

import scala.util.matching.Regex

object Utils {
  def canBeDate(str: String) = DefaultFormats.dateFormat.parse(str).isDefined

  val ScalaKeywords =
    Set("type", "val", "def", "else", "if", "object",
      "yield", "for", "import", "match", "case", "yield",
      "var", "class", "package", "extends")

  /** adds backticks for names with characters like spaces etc
    */
  def quotedName(name: String) = {
    if (ScalaKeywords(name)) '`' + name + '`'
    else if (name.matches("[a-zA-Z_][\\w\\d_]*")) name
    else '`' + name + '`'
  }

  def toUpperCamel(str: String) = {
    val WordStart = "(^|[-_ \\.]+)(\\w)".r
    WordStart.replaceAllIn(str, _ match {
      case Regex.Groups(wordBreak, firstChar) =>
        firstChar.toUpperCase // discard break and upper first char
    })
  }

  val singularToPlural: Map[String, Seq[String]] =
    io.Source.fromURL(getClass.getResource("nouns.csv")).getLines().map { line =>
      line.split("\t") match {
        case Array(singular, plurals) =>
          singular -> plurals.split(",").toSeq
        case _ => throw new Error("Invalid line: " + line)
      }
    }.toMap

  val pluralToSingular: Map[String, String] = {
    for ((s, ps) <- singularToPlural; p <- ps) yield p -> s
  }.toMap

  object Regexes {
    // splits the last word from everything else
    val LastWordRegex = "(.*\\b(?:[A-Za-z][a-z]+)*)([A-Z][a-z]+)".r
    // matches special -y words that end in ies, like fly=>flies
    val EndsWithIes   = "(.*[b-z&&[^eiou]])ies".r
    // matches special words that end in es, like church=>churches
    val EndsWithEs    = "(.*(?:[szx]|[cs]h))es".r
    // matches words that end in s, like foo=>foos
    val EndsWithS     = "(.*)s".r
  }

  def toSingular(word: String) = {
    import Regexes._
    word match {
      case LastWordRegex(init, lastWord) =>
        val singularLastWord = pluralToSingular.get(lastWord.toLowerCase) match {
          case Some(specialCase) =>
            if (lastWord.head.isUpper) specialCase.head.toUpper + specialCase.tail
            else specialCase
          case None =>
            lastWord match {
              case EndsWithIes(base) => base + "y"
              case EndsWithEs(base)  => base
              case EndsWithS(base)   => base
              case _ => lastWord
            }
        }
        init + singularLastWord
      case _ => word
    }
  }
}
