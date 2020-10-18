package com.example.streamerskextensions.utils

import org.apache.ignite.IgniteException
import org.apache.ignite.Ignition

object ExampleNodeStartup {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    @Throws(IgniteException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        Ignition.start("examples/config/example-ignite.xml")
    }
}
