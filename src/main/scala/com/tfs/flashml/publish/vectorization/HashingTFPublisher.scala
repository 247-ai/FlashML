package com.tfs.flashml.publish.featureengineering

import com.tfs.flashml.util.{ConfigUtils, PublishUtils}

import scala.collection.mutable

/**
 * Class for publishing JS code for HashingTF.
 *
 * @since 3/23/17.
 */
object HashingTFPublisher
{

  def generateJS(numFeatures: Int, input: String, output: String, binarizer: Boolean, hashFunction: Boolean,
                 globalVar: mutable.Set[String]) =
  {
    val hashingTFJsTmp = if (hashFunction) hashingTFJS
    else new StringBuilder

    globalVar += ("" + hashingTFJsTmp)
    var hashingTFJs = new StringBuilder
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      "binarizer_" + output + " = " + binarizer.toString + ";"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var size_" +
      output + " = " + numFeatures + ";"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " +
      output + " = hashingTF("
    hashingTFJs ++= input + ",size_" + output + ",binarizer_" + output + ");"
    hashingTFJs
  }

  def rotateLeftJS: StringBuilder =
  {
    val rotateLeftJS = new StringBuilder
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function rotateLeft(k,n){"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k = -k;"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k1 = ~k>>>0;"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k1 = k1+1;"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k2 = (k1 << n)|0;"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k3 = (k1 >>> (32-n));"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var k4 = (k2 | k3);"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return k4;"
    rotateLeftJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    rotateLeftJS
  }

  def mult32sJS: StringBuilder =
  {
    val mult32sJS = new StringBuilder
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function mult32s(n,m){"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "n |= 0;"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "m |= 0;"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var nlo = n & 0xffff;"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var nhi = n - nlo;"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return ( (nhi * m | 0) + (nlo * m) ) | 0;"
    mult32sJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    mult32sJS
  }

  def mixK1JS: StringBuilder =
  {
    val mixK1JS = new StringBuilder
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function mixK1(k1){"
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "k1 = mult32s(k1,C1);"
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "k1 = rotateLeft(k1,15);"
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "k1 = mult32s(k1,C2);"
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return k1;"
    mixK1JS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    mixK1JS
  }

  def mixH1JS: StringBuilder =
  {
    val mixH1JS = new StringBuilder
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function mixH1(h1,k1){"
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 ^= k1;"
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 = rotateLeft(h1,13);"
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 = ((h1*5) + 0xe6546b64);"
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return h1;"
    mixH1JS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    mixH1JS
  }

  def fmixJS: StringBuilder =
  {
    var fmixJS = new StringBuilder
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function fmix(h1,length){"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 ^= length;"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 ^= h1 >>> 16;"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 = mult32s(h1,0x85ebca6b);"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 ^= h1 >>> 13;"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 = mult32s(h1,0xc2b2ae35);"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "h1 ^= h1 >>> 16;"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return h1;"
    fmixJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    fmixJS
  }

  def hashBytesByIntJS: StringBuilder =
  {
    var hashBytesByIntJS = new StringBuilder
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function hashBytesByInt(base," +
      "lengthInBytes,seed){"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var h1 = seed;"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var i = 0;"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "while(i<lengthInBytes){"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "var intValue = ((base[i]) |"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "(base[i+1] << 8) |"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "(base[i+2] << 16) |"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "(base[i+3] << 24));"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "var k1 = mixK1(intValue);"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "h1 = mixH1(h1,k1);"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "i +=4;"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return h1;"
    hashBytesByIntJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    hashBytesByIntJS
  }

  def hashBytesJS: StringBuilder =
  {
    var hashBytesJS = new StringBuilder
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function hashBytes(base, lengthInBytes, " +
      "seed){"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var lengthAligned = lengthInBytes - " +
      "lengthInBytes % 4;"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var h1 = hashBytesByInt(base, " +
      "lengthAligned, seed);"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var i = lengthAligned;"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "while(i<lengthInBytes){"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "var byteValue = base[i];"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "var k1 = mixK1(byteValue);"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "h1 = mixH1(h1, k1);"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "i +=1;"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return fmix(h1, lengthInBytes)"
    hashBytesJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    hashBytesJS
  }

  def toUTF8ArrayJS: StringBuilder =
  {
    var toUTF8ArrayJS = new StringBuilder
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function toUTF8Array(str){"
    //todo added typecasting to String
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var utf8 = [];"
    //toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent +3) + "var str =
    // str1.toString();"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "for (var i=0; i < str.length; i++){"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "var charcode = str.charCodeAt(i);"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "if (charcode < 0x80) utf8.push(charcode);"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "else if (charcode < 0x800){"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "utf8.push(0xc0 | (charcode >> 6),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | (charcode & 0x3f));"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "}"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "else if (charcode < 0xd800 || charcode " +
      ">= 0xe000){"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "utf8.push(0xe0 | (charcode >> 12),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | ((charcode>>6) & 0x3f),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | (charcode & 0x3f));"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "}"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "else {"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "i++;"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "charcode = 0x10000 + (((charcode & " +
      "0x3ff)<<10)"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "| (str.charCodeAt(i) & 0x3ff));"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(5) + "utf8.push(0xf0 | (charcode >>18),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | ((charcode>>12) & 0x3f),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | ((charcode>>6) & 0x3f),"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(6) + "0x80 | (charcode & 0x3f));"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "}"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return utf8;"
    toUTF8ArrayJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    toUTF8ArrayJS
  }

  def getHashIndexJS: StringBuilder =
  {
    var getHashIndexJS = new StringBuilder
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "function getHashIndex(term," +
      "numberOfFeatures){"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var seed = 42;"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var utf8 = toUTF8Array(term);"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var hashValue = hashBytes(utf8, " +
      "utf8.length, seed);"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var index = hashValue % " +
      "numberOfFeatures;"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if (index < 0){"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "index = numberOfFeatures + index;"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "return index;"
    getHashIndexJS ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    getHashIndexJS
  }

  def hashingTFJS: StringBuilder =
  {
    var hashingTFJs = new StringBuilder
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "function hashingTF(termArr," +
      "numberOfFeatures,binarizer){"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var hashedTF = {};"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var C1 = -862048943;    //0xcc9e2d51"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "var C2 = 461845907;     //0x1b873593"
    hashingTFJs ++= rotateLeftJS ++ mult32sJS ++ mixK1JS ++ mixH1JS ++ fmixJS
    hashingTFJs ++= hashBytesByIntJS ++ hashBytesJS ++ toUTF8ArrayJS ++ getHashIndexJS
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "for (var ind in termArr){"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "var hashIndex = getHashIndex" +
      "(termArr[ind],numberOfFeatures);"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "if (hashIndex in hashedTF && !binarizer){"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "hashedTF[hashIndex] = hashedTF[hashIndex] +1;"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "else{"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(4) + "hashedTF[hashIndex] = 1;"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(3) + "}"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "}"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(2) + "return hashedTF;"
    hashingTFJs ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"
    hashingTFJs
  }
}
