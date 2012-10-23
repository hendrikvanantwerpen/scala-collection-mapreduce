package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import scala.collection.generic.CanBuildFrom
import scalaz._
import Scalaz._

object Aggregators {

  trait Aggregator[Coll,Elem] {
    def nil: Coll
    def insert(c: Coll, a: Elem): Coll
  }

  implicit def MonoidAggregator[Value]
               (implicit m: Monoid[Value]) =
    new Aggregator[Value,Value] {
      override def nil = m.zero
      override def insert(c: Value, a: Value) = m.append(c,a)
    }

  implicit def SeqAggregator[Elem, Repr[X] <: GenSeq[X]]
               (implicit bf: CanBuildFrom[Repr[Elem],Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def nil = bf().result
      override def insert(c: Repr[Elem], a: Elem) =
        (c :+ a).asInstanceOf[Repr[Elem]]
    }

  implicit def SetAggregator[Elem, Repr[X] <: GenSet[X]]
               (implicit bf: CanBuildFrom[Repr[Elem],Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def nil = bf().result
      override def insert(c: Repr[Elem], a: Elem) =
        (c + a).asInstanceOf[Repr[Elem]]
    }

  implicit def Tuple2Aggregator[C1,V1,C2,V2]
               (implicit ma: Aggregator[C1,V1], mb: Aggregator[C2,V2]) =
    new Aggregator[(C1,C2),(V1,V2)] {
      override def nil = (ma.nil, mb.nil)
      override def insert(c: (C1,C2), v: (V1,V2)) =
        (ma.insert(c._1, v._1), mb.insert(c._2, v._2))
    }

  implicit def Tuple3Aggregator[C1,V1,C2,V2,C3,V3]
               (implicit ma: Aggregator[C1,V1], mb: Aggregator[C2,V2],
                         mc: Aggregator[C3,V3]) =
    new Aggregator[(C1,C2,C3),(V1,V2,V3)] {
      override def nil = (ma.nil, mb.nil, mc.nil)
      override def insert(c: (C1,C2,C3), v: (V1,V2,V3)) =
        (ma.insert(c._1,v._1), mb.insert(c._2,v._2), mc.insert(c._3,v._3))
    }

  implicit def Tuple4Aggregator[C1,V1,C2,V2,C3,V3,C4,V4]
               (implicit ma: Aggregator[C1,V1], mb: Aggregator[C2,V2],
                         mc: Aggregator[C3,V3], md: Aggregator[C4,V4]) =
    new Aggregator[(C1,C2,C3,C4),(V1,V2,V3,V4)] {
      override def nil = (ma.nil, mb.nil, mc.nil, md.nil)
      override def insert(c: (C1,C2,C3,C4), v: (V1,V2,V3,V4)) =
        (ma.insert(c._1,v._1), mb.insert(c._2,v._2),
         mc.insert(c._3,v._3), md.insert(c._4,v._4))
    }

  implicit def MapAggregator[Key, Value, Repr[K,V] <: GenMap[K,V], Elem]
               (implicit m: Aggregator[Value,Elem],
                         bf: CanBuildFrom[Repr[Key,Value],
                                          (Key,Value),
                                          Repr[Key,Value]]) =
    new Aggregator[Repr[Key,Value],(Key,Elem)] {
      override def nil = bf().result
      override def insert(c: Repr[Key,Value], a: (Key,Elem)) =
        (c + (a._1 -> c.get( a._1 )
                       .map( v => m.insert(v,a._2) )
                       .getOrElse( m.insert(m.nil,a._2) ) )
        ).asInstanceOf[Repr[Key,Value]]
    }

  class AggregatorIdentity[Coll](c: Coll) {
    def |<|[Elem](e: Elem)(implicit m: Aggregator[Coll,Elem]) = m.insert(c, e)
    def |<<|[Elem](es: GenTraversableOnce[Elem])(implicit m: Aggregator[Coll,Elem]) = (c /: es)( (c,e) => m.insert(c, e) )
  }
  implicit def mkAggregator[Coll](c: Coll) = new AggregatorIdentity[Coll](c)

}
