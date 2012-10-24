package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._

trait Zero[A] {
  def zero: A
}

trait Aggregator[A,B] extends Zero[A] {
  def insert(a: A, b: B): A
  def flatInsert(a: A, bs: GenTraversableOnce[B]): A =
    (a /: bs)( insert (_,_) )
  def unit(b: B): A =
    insert (zero,b) 
} 

trait Monoid[A] extends Aggregator[A,A] {
  def unit(a: A): A = a
} 

object Aggregator {
  import scala.collection._
  import scala.collection.generic.CanBuildFrom
  import scalaz.Scalaz._

  implicit def GenTraversableOnceZero[Repr <: GenTraversableOnce[_]]
                                     (implicit bf: CanBuildFrom[Nothing,Nothing,Repr])
                                     : Zero[Repr] =
    new Zero[Repr] {
      def apply: Repr = bf().result
    }

  implicit def GenSeqAggregator[Repr[X] <: GenSeq[X], Elem]
                               (implicit z: Zero[Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = z.zero
      override def insert(s: Repr[Elem], e: Elem) =
        (s :+ e).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSeqMonoid[Repr[X] <: GenSeq[X],Elem]
                           (implicit a: Aggregator[Repr[Elem],Elem]) =
    new Monoid[Repr[Elem]] {
      override def zero: Repr[Elem] = a.zero
      override def insert(s1: Repr[Elem], s2: Repr[Elem]) =
        (s1 ++ s2).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetAggregator[Repr[X] <: GenSet[X], Elem]
                               (implicit z: Zero[Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = z.zero
      override def insert(c: Repr[Elem], e: Elem) =
        (c + e)
        .asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetMonoid[Repr[X] <: GenSet[X], Elem]
                           (implicit a: Aggregator[Repr[Elem],Elem]) =
    new Monoid[Repr[Elem]] {
      override def zero: Repr[Elem] = a.zero
      override def insert(s1: Repr[Elem], s2: Repr[Elem]) =
        (s1 ++ s2).asInstanceOf[Repr[Elem]]
    }

  implicit def GenMapAggregator[Repr[K,V] <: GenMap[K,V], Key, Value, Elem]
                               (implicit z: Zero[Repr[Key,Value]],
                                         va: Aggregator[Value,Elem]) =
    new Aggregator[Repr[Key,Value],(Key,Elem)] {
      override def zero: Repr[Key,Value] = z.zero
      override def insert(m: Repr[Key,Value], e: (Key,Elem)) =
        (m + (e._1 -> m.get( e._1 )
                       .map( v => va.insert(v,e._2) )
                       .getOrElse( va.insert(va.zero,e._2) )))
        .asInstanceOf[Repr[Key,Value]]
    }

  implicit def GenMapMonoid[Repr[K,V] <: GenMap[K,V], Key, Value]
                           (implicit a: Aggregator[Repr[Key,Value],(Key,Value)]) =
    new Monoid[Repr[Key,Value]] {
      override def zero: Repr[Key,Value] = a.zero
      override def insert(m1: Repr[Key,Value], m2: Repr[Key,Value]) =
       (m1 /: m2)( (m:Repr[Key,Value], e2:(Key,Value)) => a insert (m,e2) )
    }

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

  implicit def ScalazMonoidAggregator[A]
                                     (implicit m: scalaz.Monoid[A]) =
    new Monoid[A] {
      def zero = m.zero
      def insert(a1: A, a2: A)  = m append (a1,a2)
    }

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
