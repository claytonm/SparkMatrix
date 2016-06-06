package SparkMatrix

import org.apache.spark.rdd.RDD

class Matrix(rdd: RDD[(Int, Int, Double)]) {

  def getRDD = rdd

  def transpose(): Matrix = {
    new Matrix(rdd.
      map{case (rowIndex, colIndex, value) => (colIndex, rowIndex, value)})
  }

  def *(that: Matrix): Matrix = {

    val rddL = rdd.
      map{case (rowIndex, colIndex, value) => (colIndex, (rowIndex, value))}
    val rddR = that.getRDD.
      map{case (rowIndex, colIndex, value) => (rowIndex, (colIndex, value))}

    val productRDD = rddL.join(rddR).
      map{case (colIndexLeft, ((rowIndexLeft, valLeft), (colIndexRight, valueRight)))
      => ((rowIndexLeft, colIndexRight), valLeft*valueRight)}.
      reduceByKey(_ + _).
      map{case ((rowIndex, colIndex), value) => (rowIndex, colIndex, value)}

    new Matrix(productRDD)
  }

  def rowSum(): RDD[(Int, Double)] = {
    rdd.
      map{case (rowIndex, colIndex, value) => (rowIndex, value)}.
      reduceByKey(_ + _)
  }

  def colSum(): RDD[(Int, Double)] = {
    rdd.
      map{case (rowIndex, colIndex, value) => (colIndex, value)}.
      reduceByKey(_ + _)
  }

  def functionApply(function: Double => Double): Matrix = {

    val newRDD = rdd.
      map{case (rowIndex, colIndex, value) => (rowIndex, colIndex, function(value))}

    new Matrix(newRDD)
  }

  def getRow(row: Int): Matrix = {
    new Matrix(
      rdd.
        filter{case (rowIndex, colIndex, value) => rowIndex == row}
    )
  }
}

object Matrix {

  def rddToMatrix(rdd: RDD[Array[Double]]): Matrix = {

    val matrixRDD = rdd.
      zipWithIndex.
      map{case (vector, rowIndex) => (rowIndex, vector)}.
      flatMapValues(l => l.zipWithIndex).
      map{case (rowIndex, (value, colIndex)) => (rowIndex.toInt, colIndex.toInt, value)}

    new Matrix(matrixRDD)
  }

  def rddToSparseMatrix(rdd: RDD[Array[Double]]): Matrix = {

    val matrixRDD = rdd.
      zipWithIndex.
      map{case (vector, rowIndex) => (rowIndex, vector)}.
      flatMapValues(l => l.zipWithIndex).
      filter{case (rowIndex, (value, colIndex)) => value > 0}.
      map{case (rowIndex, (vector, colIndex)) => (rowIndex.toInt, colIndex.toInt, vector)}

    new Matrix(matrixRDD)
  }

  def sparseRDDToSparseMatrix(rdd: RDD[(Int, Int, Double)]): Matrix = {
    // input: RDD of tuples of form (Row Index, Column Index, Value).
    new Matrix(rdd)
  }

  def diag(rdd: RDD[(Int, Double)]): Matrix = {

    val diagMatrix = rdd.
      map{case (index, value) => (index, index, value)}

    new Matrix(diagMatrix)
  }
}

