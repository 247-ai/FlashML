package com.tfs.flashml.publish.preprocessing

import org.slf4j.LoggerFactory

/**
 * Class for publishing JS code for Porter Stemmer.
 *
 * @since 3/28/17.
 */
object PorterStemmingPublisher
{

  private val log = LoggerFactory.getLogger(getClass)

  def generateJS(input: String, output: String, pattern: String) =
  {
    val stemmingJs = new StringBuilder
    stemmingJs
  }

}
