package eu.stratosphere.procrustes.datagen

abstract class DataGenerator {
  val SEED = 5431423142056L

  def run(): Unit
}

