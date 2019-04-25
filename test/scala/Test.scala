package test.scala

object Test {
  def main(args: Array[String]): Unit = {
    var a: Vector[Int] = Vector.empty :+ 1 :+ 2
    var b: Vector[Int] = Vector.empty :+ 2 :+ 3
    print(a +: b)

  }
}
