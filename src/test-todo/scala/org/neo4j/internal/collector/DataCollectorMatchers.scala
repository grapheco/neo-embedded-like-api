/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.collector

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.helpers.Neo4jJavaCCParserWithFallback
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.scalatest.Matchers.equal
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.time.ZonedDateTime
import scala.collection.mutable.ArrayBuffer
import scala.reflect.Manifest

/**
 * Matchers allowing more flexible matching on results from RewindableExecutionResult.
 */
object DataCollectorMatchers {

  private val preParser = new PreParser(
    CypherConfiguration.fromConfig(Config.defaults()),
    0,
    TestExecutorCaffeineCacheFactory)

  /**
   * Matches a ZonedDateTime if it occurs between (inclusive) to given points in time.
   */
  def occurBetween(before: ZonedDateTime, after: ZonedDateTime) = BetweenMatcher(before, after)

  case class BetweenMatcher(before: ZonedDateTime, after: ZonedDateTime) extends Matcher[ZonedDateTime] {

    if (!beforeOrEqual(before, after))
      throw new IllegalArgumentException(s"Erroneous test setup: after ($after) occurs before ($before)")

    private def beforeOrEqual(lhs: ZonedDateTime, rhs: ZonedDateTime) = lhs.isBefore(rhs) || lhs.equals(rhs)

    override def apply(left: ZonedDateTime): MatchResult = {
      val lowerBound = beforeOrEqual(before, left)
      val upperBound = beforeOrEqual(left, after)
      val failMsg =
        if (!lowerBound) s"Expected $left to occur between $before and $after, but was before."
        else s"Expected $left to occur between $before and $after, but was after."

      MatchResult(
        matches = lowerBound && upperBound,
        rawFailureMessage = failMsg,
        rawNegatedFailureMessage = s"Expected $left not to occur between $before and $after.")
    }
  }

  /**
   * Matches a scala Map if it contains the expected kay-value pairs. Any additional data is ignored.
   *
   * Note that any expected value that is a matcher will be used as such, while other expected values will
   * be asserted on using regular equality.
   */
  def beMapContaining(expected: (String, Any)*) = MapMatcher(expected, exact = false)

  /**
   * Matches a scala Map if it contains exactly the expected key-value pairs.
   *
   * Note that any expected value that is a matcher will be used as such, while other expected values will
   * be asserted on using regular equality.
   */
  def beMap(expected: (String, Any)*) = MapMatcher(expected, exact = true)

  case class MapMatcher(expecteds: Seq[(String, Any)], exact: Boolean) extends Matcher[AnyRef] {

    override def apply(left: AnyRef): MatchResult = {
      val errors = new ArrayBuffer[String]
      left match {
        case m: Map[String, AnyRef] =>
          for ((key, expected) <- expecteds) {
            m.get(key) match {
              case None =>
                errors += s"Expected map to contain '$key', but didn't"
              case Some(value) =>
                expected match {
                  case m: Matcher[AnyRef] =>
                    val matchResult = m.apply(value)
                    if (!matchResult.matches)
                      errors += s"Error matching value for key '$key': \n" + matchResult.failureMessage
                  case expectedValue =>
                    if (!arraySafeEquals(value, expectedValue))
                      errors += s"Expected value '$expectedValue' for key '$key', but got '$value'"
                }
            }
          }

          if (exact && expecteds.size < m.size)
            errors += s"Unwanted keys ${m.keySet -- expecteds.map(_._1)} in map '$m'"
        case x =>
          errors += s"Expected map but got '$x'"
      }

      MatchResult(
        matches = errors.isEmpty,
        rawFailureMessage = "Encountered a bunch of errors: " + errors.mkString("\n"),
        rawNegatedFailureMessage = ""
      )
    }

    override def toString(): String = s"map(${expecteds.map(t => t._1 + ": " + t._2).mkString(", ")})"
  }

  /**
   * Matches a scala Seq if it contains all the expected values. Any additional data is ignored.
   *
   * Note that any expected value that is a matcher will be used as such, while other expected values will
   * be asserted on using regular equality.
   */
  def beListWithoutOrder(expected: Any*): WithoutOrderMatcher = WithoutOrderMatcher(expected)

  case class WithoutOrderMatcher(expected: Seq[Any]) extends Matcher[AnyRef] {

    override def apply(left: AnyRef): MatchResult = {
      val errors = new ArrayBuffer[String]
      left match {
        case values: Seq[AnyRef] =>
          for (expectedValue <- expected) {
            val contained = contains(values, expectedValue)
            if (contained.isLeft) {
              errors +=
                (expectedValue match {
                  case _: Matcher[AnyRef] => s"No value matching ${contained.left}"
                  case _ => s"Expected value '$expectedValue' in list, but wasn't there"
                })
            }
          }
        case x =>
          errors += s"Expected list but got '$x'"
      }
      MatchResult(
        matches = errors.isEmpty,
        rawFailureMessage = s"Encountered following mismatches in $left:\n{0}",
        rawNegatedFailureMessage = s"All matchers matched on $left",
        args = IndexedSeq(errors.mkString("\n")),
      )
    }

    override def toString(): String = s"list without order(${expected.mkString(", ")})"
  }

  /**
   * Matches a scala Seq if it contains exactly the expected values, in order.
   *
   * Note that any expected value that is a matcher will be used as such, while other expected values will
   * be asserted on using regular equality.
   */
  def beListInOrder(expected: Any*) = InOrderMatcher(expected)

  case class InOrderMatcher(expected: Seq[Any]) extends Matcher[AnyRef] {

    override def apply(left: AnyRef): MatchResult = {
      val errors = new ArrayBuffer[String]
      left match {
        case values: Seq[AnyRef] =>

          for (i <- expected.indices) {
            val expectedValue = expected(i)

            if (i < values.size) {
              val value = values(i)
              val found =
                expectedValue match {
                  case m: Matcher[AnyRef] =>
                    m.apply(value).matches
                  case something =>
                    arraySafeEquals(value, something)
                }

              if (!found)
                errors += s"Expected value '$expectedValue' at position $i, but was '$value'"
            } else
              errors += s"Expected value '$expectedValue' at position $i, but list was too small"
          }
          if (values.size > expected.size)
            errors += s"Expected list of ${expected.size} elements, but got additional elements ${values.slice(expected.size, values.size)}"

        case x =>
          errors += s"Expected list but got '$x'"
      }
      MatchResult(
        matches = errors.isEmpty,
        rawFailureMessage = "Encountered a bunch of errors: " + errors.map("  "+_).mkString("\n", "\n", "\n"),
        rawNegatedFailureMessage = "BAH"
      )
    }

    override def toString(): String = s"list in order(${expected.mkString(", ")})"
  }

  def contains(left: AnyRef, expected: Any): Either[String, AnyRef] = {
    var error = ""
    var matchedElement: Option[AnyRef] = None
    left match {
      case values: Seq[AnyRef] =>
        val matcher: Matcher[AnyRef] = expected match {
          case m: Matcher[AnyRef] => m
          case expectedValue => equal(expectedValue)
        }
        matchedElement = values.find(matcher(_).matches)
        if (matchedElement.isEmpty) {
          error = s"Expected value '$values' to contain $matcher, but did not"
        }

      case x =>
        error = s"Expected list but got '$x'"
    }
    if (matchedElement.isDefined) {
      Right(matchedElement.get)
    } else {
      Left(error)
    }
  }

  def containListElement(expected: Any): ContainListElementMatcher = ContainListElementMatcher(expected)

  case class ContainListElementMatcher(expected: Any) extends Matcher[AnyRef] {

    override def apply(left: AnyRef): MatchResult = {
      val result = contains(left, expected)
      MatchResult(
        matches = result.isRight,
        rawFailureMessage = s"Encountered following mismatches in $left:\n{0}",
        rawNegatedFailureMessage = s"the following element in $left was found, which matches:\n{1}",
        args = IndexedSeq(result.left.getOrElse(""), result.right.getOrElse("")),
      )
    }

    override def toString(): String = s"list containing($expected)"
  }

  /**
   * Matches instances of the specified type.
   */
  def ofType[T : Manifest]: OfTypeMatcher[T] =
    OfTypeMatcher[T](manifest.erasure.asInstanceOf[Class[T]])

  case class OfTypeMatcher[T](clazz: Class[T]) extends Matcher[AnyRef] {

    override def apply(left: AnyRef): MatchResult =
      MatchResult(
        matches = left.isInstanceOf[T],
        rawFailureMessage = s"'$left' is not an instance of '${clazz.getSimpleName}'",
        rawNegatedFailureMessage = "")

    override def toString(): String = s"of type(${clazz.getSimpleName})"
  }

  /**
   * Matches instances of the specified type.
   */
  def beCypher(text: String): BeCypherMatcher = BeCypherMatcher(text)

  case class BeCypherMatcher(expected: String) extends Matcher[AnyRef] {

    val parser = Neo4jJavaCCParserWithFallback
    private val preParsedQuery: PreParsedQuery = preParser.preParseQuery(expected, profile = false)
    private val expectedAst = parser.parse(preParsedQuery.statement, Neo4jCypherExceptionFactory(expected, Some(preParsedQuery.options.offset)), new AnonymousVariableNameGenerator)

    override def apply(left: AnyRef): MatchResult =
      MatchResult(
        matches = left match {
          case text: String =>
            val preParsedQuery1 = preParser.preParseQuery(text, profile = false)
            parser.parse(preParsedQuery1.statement, Neo4jCypherExceptionFactory(text, Some(preParsedQuery1.options.offset)), new AnonymousVariableNameGenerator) == expectedAst
          case _ => false
        },
        rawFailureMessage = s"'$left' is not the same Cypher as '$expected'",
        rawNegatedFailureMessage = s"'$left' is unexpectedly the same Cypher as '$expected'")

    override def toString(): String = s"cypher string `$expected`"
  }

  /**
   * Extract a nested value from a tree of maps.
   */
  def subMap(res: Map[String, AnyRef], keys: String*): AnyRef = {
    def inner(res: Map[String, AnyRef], keys: List[String]): AnyRef =
      keys match {
        case Nil => res
        case key :: Nil => res(key)
        case key :: rest =>
          res(key) match {
            case m: Map[String, AnyRef] => inner(m, rest)
            case notMap =>
              throw new IllegalArgumentException(s"Expected map but got '$notMap'")
          }
      }
    inner(res, keys.toList)
  }

  /**
   * Check whether a == b, but performs element equality on Arrays, instead of instance equality.
   */
  def arraySafeEquals(a: AnyRef, b: Any): Boolean = {
    a match {
      case arr: Array[_] =>
        b match {
          case brr: Array[_] => arr.deep == brr.deep
          case _ => arr.deep == b
        }
      case _ => {
        b match {
          case brr: Array[_] => a == brr.deep
          case _ => a == b
        }
      }
    }
  }
}
