package com.tfs.flashml.publish.preprocessing

import org.slf4j.LoggerFactory

/**
 * Class for publishing JS code for word class replacement.
 *
 * @since 3/28/17.
 */
object WordClassReplacementPublisher
{

    private val log = LoggerFactory.getLogger(getClass)

    def generateJS(wordClasses: Array[(String, String)], input: String, output: String) =
    {
        val tokenizerJs = new StringBuilder
        tokenizerJs
    }

}
