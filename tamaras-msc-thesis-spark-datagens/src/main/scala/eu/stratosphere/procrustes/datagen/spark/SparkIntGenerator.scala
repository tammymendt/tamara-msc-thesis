package eu.stratosphere.procrustes.datagen.spark

import eu.stratosphere.procrustes.datagen.util.Distributions._
import eu.stratosphere.procrustes.datagen.util.RanHash
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

class SparkIntGenerator(master: String, numTasks: Int, tuplesPerTask: Int, keyDist: Distribution, pay: Int, output: String) {

  import SparkIntGenerator.SEED

  def run() = {
    val conf = new SparkConf().setAppName("integer-generator").setMaster(master)
    val sc = new SparkContext(conf)
    val n = tuplesPerTask
    val s = new Random(SEED).nextString(pay)
    val seed = SEED
    val kd = this.keyDist

    val dataset = sc.parallelize(0 until numTasks, numTasks).flatMap(i => {
      val partitionStart = n * i // the index of the first point in the current partition
      val randStart = partitionStart
      val rand = new RanHash(seed)
      rand.skipTo(seed + randStart)

      for (j <- partitionStart until (partitionStart + n)) yield {
        Math.round(kd.sample(rand))
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
    val TruncIntPareto = """TruncPareto\[(\d+),(\d+)\]""".r
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      throw new RuntimeException("Arguments count != 6")
    }

    val master: String = args(0)
    val numTasks: Int = args(1).toInt
    val tuplesPerTask: Int = args(2).toInt
    val keyDist: Distribution = parseDist(args(3))
    val pay: Int = args(4).toInt
    val output: String = args(5)
    val generator = new SparkIntGenerator(master, numTasks, tuplesPerTask, keyDist, pay, output)
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

