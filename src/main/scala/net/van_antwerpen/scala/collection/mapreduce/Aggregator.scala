package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._

trait Aggregator[A,B] {
  def zero: A
  def insert(a: A, b: B): A
  def flatInsert(a: A, bs: GenTraversableOnce[B]): A =
    (a /: bs)( this insert (_,_) )
  def unit(b: B): A =
    insert (zero,b) 
} 

object Aggregator {
  
  class AggregatorIdentity[A](a: A) {

    def |<|[B](b: B)
              (implicit agg: Aggregator[A,B]) =
      agg insert (a, b)

    def |<<|[B](bs: GenTraversableOnce[B])
               (implicit agg: Aggregator[A,B]) =
      agg flatInsert (a, bs)

  }

  implicit def mkAggregator[Coll](c: Coll) = new AggregatorIdentity[Coll](c)

}
