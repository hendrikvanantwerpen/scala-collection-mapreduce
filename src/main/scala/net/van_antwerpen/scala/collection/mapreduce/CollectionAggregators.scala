package net.van_antwerpen.scala.collection.mapreduce

object CollectionAggregators {
  import scala.collection._
  import scala.collection.generic.CanBuildFrom

  implicit def GenSeqAggregator[Repr[X] <: GenSeq[X], Elem]
                               (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s: Repr[Elem], e: Elem) =
        (s :+ e).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSeqMonoid[Repr[X] <: GenSeq[X], Elem]
                           (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Repr[Elem]] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s1: Repr[Elem], s2: Repr[Elem]) =
        (s1 ++ s2).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetAggregator[Repr[X] <: GenSet[X], Elem]
                               (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = bf().result
      override def insert(c: Repr[Elem], e: Elem) =
        (c + e).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetMonoid[Repr[X] <: GenSet[X], Elem]
                           (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Repr[Elem]] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s1: Repr[Elem], s2: Repr[Elem]) =
        (s1 ++ s2).asInstanceOf[Repr[Elem]]
    }

  implicit def GenMapAggregator[Repr[K,V] <: GenMap[K,V], Key, Value, Elem]
                               (implicit bf: CanBuildFrom[Nothing,(Key,Value),Repr[Key,Value]],
                                          va: Aggregator[Value,Elem]) =
    new Aggregator[Repr[Key,Value],(Key,Elem)] {
      override def zero: Repr[Key,Value] = bf().result
      override def insert(m: Repr[Key,Value], e: (Key,Elem)) =
        (m + (e._1 -> m.get( e._1 )
                       .map( v => va insert (v,e._2) )
                       .getOrElse( va insert (va.zero,e._2) )))
        .asInstanceOf[Repr[Key,Value]]
    }

  implicit def GenMapMonoid[Repr[K,V] <: GenMap[K,V], Key, Value]
                           (implicit bf: CanBuildFrom[Nothing,(Key,Value),Repr[Key,Value]],
                                      va: Aggregator[Repr[Key,Value],(Key,Value)]) =
    new Aggregator[Repr[Key,Value],Repr[Key,Value]] {
      override def zero: Repr[Key,Value] = bf().result
      override def insert(m1: Repr[Key,Value], m2: Repr[Key,Value]) =
       (m1 /: m2)( (m:Repr[Key,Value], e2: (Key,Value)) => va insert (m,e2) )
    }
  
}