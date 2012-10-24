package net.van_antwerpen.scala.collection.mapreduce

import scalaz._
import Scalaz._

abstract class Aggregator[Coll,Elem]
               (implicit cm: Monoid[Coll]) {
  val zero = cm.zero
  def append(c1: Coll, c2: => Coll) = cm append (c1, c2)
  def insert(c: Coll, e: Elem): Coll
}

object Aggregator {
  import scala.collection._
  import scala.collection.generic.CanBuildFrom
  import scalaz._
  import Scalaz._
  import Monoid._

  implicit def MonoidAggregator[Coll]
               (implicit mm: Monoid[Coll]) =
    new Aggregator[Coll,Coll] {
      override def insert(c1: Coll, c2: Coll) = mm.append(c1,c2)
    }

  implicit def GenSeqAggregator[Elem, Repr[X] <: GenSeq[X]]
                               (implicit mm: Monoid[Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def insert(c: Repr[Elem], e: Elem) =
        (c :+ e)
        .asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetAggregator[Elem, Repr[X] <: GenSet[X]]
                               (implicit mm: Monoid[Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def insert(c: Repr[Elem], e: Elem) =
        (c + e)
        .asInstanceOf[Repr[Elem]]
    }

  implicit def MapAggregator[Key, Value, Repr[K,V] <: GenMap[K,V], Elem]
               (implicit mm: Monoid[Repr[Key,Value]],
                         va: Aggregator[Value,Elem],
                         bf: CanBuildFrom[Repr[Key,Value],(Key,Value),Repr[Key,Value]]) =
    new Aggregator[Repr[Key,Value],(Key,Elem)] {
      override def insert(c: Repr[Key,Value], e: (Key,Elem)) =
        (c + (e._1 -> c.get( e._1 )
                       .map( v => va.insert(v,e._2) )
                       .getOrElse( va.insert(va.zero,e._2) )))
        .asInstanceOf[Repr[Key,Value]]
    }

  implicit def Tuple2Aggregator[C1,V1,C2,V2]
               (implicit mm: Monoid[(C1,C2)],
                         m1: Aggregator[C1,V1],
                         m2: Aggregator[C2,V2]) =
    new Aggregator[(C1,C2),(V1,V2)] {
      override def insert(c: (C1,C2), v: (V1,V2)) =
        (m1.insert(c._1, v._1),
         m2.insert(c._2, v._2))
    }

  implicit def Tuple3Aggregator[C1,V1,C2,V2,C3,V3]
               (implicit mm: Monoid[(C1,C2,C3)],
                         m1: Aggregator[C1,V1],
                         m2: Aggregator[C2,V2],
                         m3: Aggregator[C3,V3]) =
    new Aggregator[(C1,C2,C3),(V1,V2,V3)] {
      override def insert(c: (C1,C2,C3), v: (V1,V2,V3)) =
        (m1.insert(c._1,v._1),
         m2.insert(c._2,v._2),
         m3.insert(c._3,v._3))
    }

  implicit def Tuple4Aggregator[C1,V1,C2,V2,C3,V3,C4,V4]
               (implicit mm: Monoid[(C1,C2,C3,C4)],
                         m1: Aggregator[C1,V1],
                         m2: Aggregator[C2,V2],
                         m3: Aggregator[C3,V3],
                         m4: Aggregator[C4,V4]) =
    new Aggregator[(C1,C2,C3,C4),(V1,V2,V3,V4)] {
      override def insert(c: (C1,C2,C3,C4), v: (V1,V2,V3,V4)) =
        (m1.insert(c._1,v._1),
         m2.insert(c._2,v._2),
         m3.insert(c._3,v._3),
         m4.insert(c._4,v._4))
    }

  class AggregatorIdentity[Coll](c: Coll) {

    def |<|[Elem](e: Elem)
                 (implicit m: Aggregator[Coll,Elem]) =
      m.insert(c, e)

    def |<<|[Elem](es: GenTraversableOnce[Elem])
                  (implicit m: Aggregator[Coll,Elem]) =
      (c /: es)( (c,e) => m.insert(c, e) )

  }

  implicit def mkAggregator[Coll](c: Coll) = new AggregatorIdentity[Coll](c)

}
