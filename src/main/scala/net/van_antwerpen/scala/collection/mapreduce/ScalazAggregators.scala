package net.van_antwerpen.scala.collection.mapreduce

import scalaz._

object ScalazAggregators {

  implicit def ScalazMonoidAggregator[A]
                                     (implicit m: Monoid[A]) =
    new Aggregator[A,A] {
      def zero = m.zero
      def insert(a1: A, a2: A)  = m append (a1,a2)
    }

}