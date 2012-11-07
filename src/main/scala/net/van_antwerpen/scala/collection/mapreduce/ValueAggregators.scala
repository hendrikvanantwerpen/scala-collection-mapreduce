package net.van_antwerpen.scala.collection.mapreduce

object ValueAggregators {
  
  implicit def Tuple2Aggregator[A1,A2,B1,B2]
               (implicit a1: Aggregator[A1,B1],
                         a2: Aggregator[A2,B2]) =
    new Aggregator[(A1,A2),(B1,B2)] {
      override def zero = (a1.zero,a2.zero)
      override def insert(a: (A1,A2), b: (B1,B2)) =
        (a1.insert(a._1, b._1),
         a2.insert(a._2, b._2))
    }

  implicit def Tuple3Aggregator[A1,A2,A3,B1,B2,B3]
               (implicit a1: Aggregator[A1,B1],
                         a2: Aggregator[A2,B2],
                         a3: Aggregator[A3,B3]) =
    new Aggregator[(A1,A2,A3),(B1,B2,B3)] {
      override def zero = (a1.zero,a2.zero,a3.zero)
      override def insert(a: (A1,A2,A3), b: (B1,B2,B3)) =
        (a1.insert(a._1,b._1),
         a2.insert(a._2,b._2),
         a3.insert(a._3,b._3))
    }

  implicit def Tuple4Aggregator[A1,A2,A3,A4,B1,B2,B3,B4]
               (implicit a1: Aggregator[A1,B1],
                         a2: Aggregator[A2,B2],
                         a3: Aggregator[A3,B3],
                         a4: Aggregator[A4,B4]) =
    new Aggregator[(A1,A2,A3,A4),(B1,B2,B3,B4)] {
      override def zero = (a1.zero,a2.zero,a3.zero,a4.zero)
      override def insert(a: (A1,A2,A3,A4), b: (B1,B2,B3,B4)) =
        (a1.insert(a._1,b._1),
         a2.insert(a._2,b._2),
         a3.insert(a._3,b._3),
         a4.insert(a._4,b._4))
    }

  implicit def SumMonoid =
    new Aggregator[Int,Int] {
      override def zero = 0
      override def insert(a: Int, b: Int) = a + b
    }

  implicit def StringMonoid =
    new Aggregator[String,String] {
      override def zero = ""
      override def insert(a: String, b: String) = a + b
    }
  
}