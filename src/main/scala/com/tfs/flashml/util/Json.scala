package com.tfs.flashml.util

/**
  * Single class JSON parser
  * Created by gaoyunxiang on 8/22/15.
  * Modified by samikrc
  * Updated 30-07-2020
  */

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable

object Json
{

    /**
      * Incomplete JSON
      * @param message
      */
    case class IncompleteJSONException(val message: String) extends Exception(s"Parse error: the JSON string might be incomplete - $message")

    /**
      * Unrecognized characters
      * @param char
      * @param pos
      */
    case class UnrecognizedCharException(val char: String, val pos: Int) extends Exception(s"Parse error: unrecognized character(s) '${char.substring(0, 6)}'... at position $pos - might be incomplete. Check for matching braces.")

    object Type extends Enumeration
    {
        val NULL, INT, DOUBLE, BOOLEAN, STRING, ARRAY, OBJECT = Value
    }

    /**
      * Encapsulates a JSON structure from any of the supported data type.
      * @param inputValue
      */
    class Value(inputValue: Any)
    {

        def asInt: Int = value match
        {
            case v: Long if v.toInt == v => v.toInt
        }

        def isInt: Boolean = value.isInstanceOf[Long] || value.isInstanceOf[Int]

        def asLong = value.asInstanceOf[Long]

        def asDouble = value.asInstanceOf[Double]

        def isDouble = value.isInstanceOf[Double]

        def asBoolean = value.asInstanceOf[Boolean]

        def isBoolean = value.isInstanceOf[Boolean]

        def asString = value.asInstanceOf[String]

        def isString = value.isInstanceOf[String]

        def asArray = value.asInstanceOf[Array[Value]]

        def isArray = value.isInstanceOf[Array[_]]

        def asMap = value.asInstanceOf[Map[String, Value]]

        def isMap = value.isInstanceOf[Map[_, _]]

        def isNumeric = isInt || isDouble

        def apply(i: Int): Value = value.asInstanceOf[Array[Value]](i)

        def apply(key: String): Value = value.asInstanceOf[Map[String, Value]](key)

        /**
          * Method to write this object as JSON string.
          * @return
          */
        def write: String =
        {
            val buffer = new mutable.StringBuilder()
            recWrite(buffer)
            buffer.toString()
        }

        /**
          * Method to write this object as JSON string, ending with newline.
          * @return
          */
        def writeln: String = s"${write}\n"

        private val value: Any = inputValue match
        {
            case null => null
            case v: Int => v.toLong
            case _: Long => inputValue
            case _: Double => inputValue
            case _: Boolean => inputValue
            case _: String => inputValue
            // Adding Float: up-converting to double
            case v: Float => v.toDouble
            case v: Map[_, _] => v.map{ case (k, v) => (k.toString, Value(v)) }
            // Adding mutable map classes
            // Need separate entry for LinkedHashMap, otherwise we loose type.
            case v: mutable.LinkedHashMap[_,_] => v.map{ case (k, v) => (k.toString, Value(v)) }
            case v: mutable.Map[_, _] => v.map{ case (k, v) => (k.toString, Value(v)) }
            // Adding mutable and immutable collection classes
            case v: mutable.Iterable[_] => v.map(Value(_)).toArray
            case v: immutable.Iterable[_] => v.map(Value(_)).toArray
            //case v: Vector[_] => v.map(Value(_)).toArray
            //case v: List[_] => v.map(Value(_)).toArray
            case v: Array[_] => v.map(Value(_))
            case v: Iterator[_] => v.map(Value(_)).toArray

            case v: Value => v.value
            case _ => throw new Exception("Unknown type")
        }

        private def recWrite(buffer: mutable.StringBuilder): Unit =
        {
            value match
            {
                case null => buffer.append("null")
                case v: Long => buffer.append(v)
                case v: Boolean => buffer.append(v)
                case v: Double => buffer.append(v)
                case v: String =>
                    buffer.append('"')
                    v.foreach
                    {
                        each =>
                        {
                            if (each == '\\' || each == '"')
                            {
                                buffer.append('\\')
                            }
                            else if (each == '\b')
                            {
                                buffer.append("\\b")
                            }
                            else if (each == '\f')
                            {
                                buffer.append("\\f")
                            }
                            else if (each == '\n')
                            {
                                buffer.append("\\n")
                            }
                            else if (each == '\r')
                            {
                                buffer.append("\\r")
                            }
                            else if (each == '\t')
                            {
                                buffer.append("\\t")
                            }
                            else
                            {
                                buffer.append(each)
                            }
                        }
                    }
                    buffer.append('"')
                case v: Array[_] =>
                    buffer.append('[')
                    for (i <- v.indices)
                    {
                        if (i != 0)
                        {
                            buffer.append(',')
                        }
                        v(i).asInstanceOf[Value].recWrite(buffer)
                    }
                    buffer.append(']')
                case v: Map[_, _] =>
                    buffer.append('{')
                    var first = true
                    v.foreach
                    {
                        case one =>
                            if (!first)
                            {
                                buffer.append(',')
                            }
                            first = false
                            buffer.append('"')
                            buffer.append(one._1)
                            buffer.append('"')
                            buffer.append(':')
                            one._2.asInstanceOf[Value].recWrite(buffer)
                    }
                    buffer.append('}')
                case v: mutable.LinkedHashMap[_, _] =>
                    buffer.append('{')
                    var first = true
                    v.foreach
                    {
                        case one =>
                            if (!first)
                            {
                                buffer.append(',')
                            }
                            first = false
                            buffer.append('"')
                            buffer.append(one._1)
                            buffer.append('"')
                            buffer.append(':')
                            one._2.asInstanceOf[Value].recWrite(buffer)
                    }
                    buffer.append('}')
                case _ => throw new Exception("Unknown data type")
            }
        }

        override def toString: String =
        {
            // Only convert to string if this is a single value
            if(this.isInt) this.asInt.toString
            else if(this.isDouble) this.asDouble.toString
            else if(this.isString) this.asString
            else if(this.isArray || this.isMap) super.toString
            else this.asLong.toString
        }

        /**
          * Method to get the Json.Value object given a path in x.y.z format. Only works for
          * successive maps with keys as "x", "y", "z" etc.
          * @param path
          * @return
          */
        def get(path: String): Json.Value =
        {
            @tailrec
            def getVal(map: Map[String, Value], parts: List[String]): Value =
            {
                if(parts.length == 1)
                    map(parts(0))
                else
                    getVal(map(parts(0)).asMap, parts.splitAt(1)._2)
                /*
                // Not sure how to do below elegantly!!
                parts match
                {
                    case h::t => getVal(map(h).asMap, t)
                    case _ => map(parts(0))
                }
                */
            }
            getVal(this.asMap, path.split('.').toList)
        }
    }

    object Value
    {
        def apply(in: Any): Value =
        {
            new Value(in)
        }
    }

    def parse(p: String): Value =
    {
        val sta = mutable.ArrayBuffer[(Char, Any)]()
        var i = 0
        while (i < p.length)
        {
            if (p(i).isWhitespace || p(i) == '-' || p(i) == ':' || p(i) == ',')
            {
            }
            else if (p(i) == '[')
            {
                sta.append(('[', null))
            }
            else if (p(i) == '{')
            {
                sta.append(('{', null))
            }
            else if (p(i) == ']')
            {
                val vec = mutable.ArrayStack[Value]()
                while (sta.nonEmpty && sta.last._1 != '[')
                {
                    vec.push(Value(sta.last._2))
                    sta.trimEnd(1)
                }
                if (sta.isEmpty || sta.last._1 != '[')
                {
                    throw new IncompleteJSONException("[] does not match")
                }
                sta.trimEnd(1)
                sta.append(('a', vec.iterator))
            }
            else if (p(i) == '}')
            {
                val now = mutable.HashMap[String, Value]()

                while (sta.length >= 2 && sta.last._1 != '{')
                {
                    val new_value: Value = Value(sta.last._2)
                    sta.trimEnd(1)
                    val new_key = sta.last._2.asInstanceOf[String]
                    sta.trimEnd(1)
                    now.update(new_key, new_value)
                }
                if (sta.isEmpty || sta.last._1 != '{')
                {
                    throw new IncompleteJSONException("{} does not match")
                }
                sta.trimEnd(1)
                sta.append(('o', now.toMap))
            }
            else if (p(i) == '"')
            {
                var j = i + 1
                val S = new mutable.StringBuilder()
                while (j < p.length && p(j) != '"')
                {
                    if (p(j) == '\\')
                    {
                        if (p(j + 1) == 'b')
                        {
                            S.append('\b')
                        }
                        else if (p(j + 1) == 'f')
                        {
                            S.append('\f')
                        }
                        else if (p(j + 1) == 'n')
                        {
                            S.append('\n')
                        }
                        else if (p(j + 1) == 'r')
                        {
                            S.append('\r')
                        }
                        else if (p(j + 1) == 't')
                        {
                            S.append('\t')
                        }
                        else
                        {
                            S.append(p(j + 1))
                        }
                        j += 2
                    }
                    else
                    {
                        S.append(p(j))
                        j += 1
                    }
                }
                sta.append(('v', S.toString()))
                i = j
            }
            else if (p(i).isDigit)
            {
                val is_double =
                {
                    var j = i + 1
                    while (j < p.length && p(j).isDigit)
                    {
                        j += 1
                    }
                    j < p.length && (p(j) == '.' || p(j) == 'e' || p(j) == 'E')
                }
                val minus_flag = if (i > 0 && p(i - 1) == '-')
                {
                    -1
                }
                else
                {
                    1
                }
                var j = i
                while (j < p.length && p(j) != ',' && p(j) != ']' && p(j) != '}')
                {
                    j += 1
                }
                if (is_double)
                {
                    val v = p.substring(i, j).replaceAll("[^a-zA-Z0-9.]+","").toDouble * minus_flag
                    sta.append(('d', v))
                }
                else
                {
                    val v = p.substring(i, j).replaceAll("[^a-zA-Z0-9.]+","").toLong * minus_flag
                    sta.append(('i', v))
                }
                i = j - 1
            }
            else if (p.substring(i, i + 4) == "null")
            {
                sta.append(('n', null))
                i += 3
            }
            else if (p.substring(i, i + 4) == "true")
            {
                sta.append(('b', true))
                i += 3
            }
            else if (p.substring(i, i + 5) == "false")
            {
                sta.append(('b', false))
                i += 4
            }
            else
            {
                throw new UnrecognizedCharException(p.substring(i), i)
            }
            i += 1
        }
        if (sta.length != 1)
        {
            throw new IncompleteJSONException("Unknown parsing error")
        }
        Value(sta.head._2)
    }

}
