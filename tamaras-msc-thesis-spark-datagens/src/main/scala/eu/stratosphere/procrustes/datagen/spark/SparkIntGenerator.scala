package eu.stratosphere.procrustes.datagen.spark

import eu.stratosphere.procrustes.datagen.util.Distributions._
import eu.stratosphere.procrustes.datagen.util.RanHash
import org.apache.spark.{SparkConf, SparkContext}

class SparkIntGenerator(master: String, numTasks: Int, tuplesPerTask: Long, keyDist: Distribution, output: String) {

  import SparkIntGenerator.SEED

  def run() = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("integer-generator")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    val sc = new SparkContext(conf)
    val n = tuplesPerTask
    val seed = SEED
    val kd = this.keyDist

    val dataset = sc.parallelize(0 until numTasks, numTasks).flatMap(i => {
      val partitionStart = n * i // the index of the first point in the current partition

      // println(s"task $i generating the range from $partitionStart until ${partitionStart + n}")
      new Traversable[Int] {
        override def foreach[U](f: (Int) => U): Unit = {
          val rand = new RanHash(seed + partitionStart)
          for (j <- partitionStart until (partitionStart + n)) yield {
            // if (j % 100000 == 0) println(s"$i at pos $j (${(j - partitionStart) / (n * 1.0)}% ready)")
            Math.round(kd.sample(rand))
          }
        }
      }
    })

    dataset.saveAsTextFile(output)
    sc.stop()
  }
}

object SparkIntGenerator {

  val SEED = 5431423142056L

  object Patterns {
    val Uniform = """Uniform\[(\d+)\]""".r
    val Gaussian = """Gaussian\[(\d+),(\d+)\]""".r
    val Pareto = """Pareto\[(\d+)\]""".r
    val TruncIntPareto = """TruncIntPareto\[(\d+),(\d+)\]""".r
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      throw new RuntimeException("Arguments count != 6")
    }

    val master: String = args(0)
    val numTasks: Int = args(1).toInt
    val tuplesPerTask: Int = args(2).toInt
    val keyDist: Distribution = parseDist(args(3))
    val output: String = args(4)
    val generator = new SparkIntGenerator(master, numTasks, tuplesPerTask, keyDist, output)
    generator.run()
  }

  def parseDist(s: String): Distribution = s match {
    case Patterns.Pareto(a) => Pareto(a.toDouble)
    case Patterns.Gaussian(a, b) => Gaussian(a.toDouble, b.toDouble)
    case Patterns.Uniform(a) => Uniform(a.toInt)
    case Patterns.TruncIntPareto(a, b) => TruncatedIntPareto(a.toInt, b.toInt)
    case _ => Pareto(1)
  }
}

