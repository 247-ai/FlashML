package com.tfs.flashml.util

/**
  * Simple utilities for JS publishing.
  * @since 4/21/17.
  */
object PublishUtils {

  def getNewLine : String = {
    "\n"
  }

  def indent(n : Int) : String = {
    "    " * n
  }

}
