package com.example.streamerskextensions.utils

import org.apache.ignite.Ignite
import org.apache.ignite.cluster.ClusterGroup
import java.net.URL

object ExamplesUtils {
    /**  */
    private val CLS_LDR = ExamplesUtils::class.java.classLoader

    /**
     * Exits with code `-1` if maximum memory is below 90% of minimally allowed threshold.
     *
     * @param min Minimum memory threshold.
     */
    fun checkMinMemory(min: Long) {
        val maxMem = Runtime.getRuntime().maxMemory()
        if (maxMem < .85 * min) {
            System.err.println("Heap limit is too low (" + maxMem / (1024 * 1024) +
                    "MB), please increase heap size at least up to " + min / (1024 * 1024) + "MB.")
            System.exit(-1)
        }
    }

    /**
     * Returns URL resolved by class loader for classes in examples project.
     *
     * @return Resolved URL.
     */
    fun url(path: String): URL {
        return CLS_LDR.getResource(path)
                ?: throw RuntimeException("Failed to resolve resource URL by path: $path")
    }

    /**
     * Checks minimum topology size for running a certain example.
     *
     * @param grp Cluster to check size for.
     * @param size Minimum number of nodes required to run a certain example.
     * @return `True` if check passed, `false` otherwise.
     */
    fun checkMinTopologySize(grp: ClusterGroup, size: Int): Boolean {
        val prjSize = grp.nodes().size
        if (prjSize < size) {
            System.err.println(">>> Please start at least $size cluster nodes to run example.")
            return false
        }
        return true
    }

    /**
     * Checks if cluster has server nodes.
     *
     * @param ignite Ignite instance.
     * @return `True` if cluster has server nodes, `false` otherwise.
     */
    fun hasServerNodes(ignite: Ignite): Boolean {
        if (ignite.cluster().forServers().nodes().isEmpty()) {
            System.err.println("Server nodes not found (start data nodes with ExampleNodeStartup class)")
            return false
        }
        return true
    }

    /**
     * Convenience method for printing query results.
     *
     * @param res Query results.
     */
    fun printQueryResults(res: List<*>?) {
        if (res == null || res.isEmpty()) println("Query result set is empty.") else {
            for (row in res) {
                if (row is List<*>) {
                    print("(")
                    val l = row
                    for (i in l.indices) {
                        val o = l[i]!!
                        if (o is Double || o is Float) System.out.printf("%.2f", o) else print(l[i])
                        if (i + 1 != l.size) print(',')
                    }
                    println(')')
                } else println("  $row")
            }
        }
    }
}
