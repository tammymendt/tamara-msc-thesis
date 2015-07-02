package eu.stratosphere.procrustes.datagen.flink

import eu.stratosphere.procrustes.datagen.util.Distributions._
import eu.stratosphere.procrustes.datagen.util.RanHash
import org.apache.flink.util.NumberSequenceIterator

class FlinkIntGenerator(numTasks: Int, tuplesPerTask: Long, keyDist: Distribution, output: String) {

  import FlinkIntGenerator.SEED
  import org.apache.flink.api.scala._

  def run() = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val n = tuplesPerTask
    val seed = SEED
    val kd = this.keyDist

    val dataset = env
      .fromParallelCollection(new NumberSequenceIterator(1, numTasks * n))
      .setParallelism(numTasks)
      .map(x => Math.round(kd.sample(new RanHash(seed + x))))

    dataset.writeAsText(output)
    env.execute("integer-generator")
  }
}

object FlinkIntGenerator {

  val SEED = 5431423142056L

  object Patterns {
    val Uniform = """Uniform\[(\d+)\]""".r
    val Gaussian = """Gaussian\[(\d+),(\d+)\]""".r
    val Pareto = """Pareto\[(\d+)\]""".r
    val TruncIntPareto = """TruncIntPareto\[(\d+),(\d+)\]""".r
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new RuntimeException("Arguments count != 6")
    }

    val numTasks: Int = args(0).toInt
    val tuplesPerTask: Int = args(1).toInt
    val keyDist: Distribution = parseDist(args(2))
    val output: String = args(3)
    val generator = new FlinkIntGenerator(numTasks, tuplesPerTask, keyDist, output)
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

