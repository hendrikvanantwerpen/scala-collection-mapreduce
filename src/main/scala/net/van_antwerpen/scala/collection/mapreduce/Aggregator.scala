package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find Aggregator for ${A} inserting ${B}")
trait Aggregator[A,B] {
  def zero: A
  def insert(a: A, b: B): A
  def unit(b: B): A =
    insert (zero,b) 
} 

object Aggregator {
  
  import scala.collection._
  import scala.collection.generic.CanBuildFrom

  implicit def GenSeqAggregator[Repr[X] <: GenSeq[X], Elem]
                               (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s: Repr[Elem], e: Elem) =
        (s :+ e).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSeqMonoid[Repr[X] <: GenSeq[X], Elem, In[X] <: GenTraversableOnce[X]]
                           (implicit bf: CanBuildFrom[GenSeq[Elem],Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],In[Elem]] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s1: Repr[Elem], s2: In[Elem]) =
        (s1.++(s2)(bf))
    }

  implicit def GenSetAggregator[Repr[X] <: GenSet[X], Elem]
                               (implicit bf: CanBuildFrom[Nothing,Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],Elem] {
      override def zero: Repr[Elem] = bf().result
      override def insert(c: Repr[Elem], e: Elem) =
        (c + e).asInstanceOf[Repr[Elem]]
    }

  implicit def GenSetMonoid[Repr[X] <: GenSet[X], Elem, In[X] <: GenTraversableOnce[X]]
                           (implicit bf: CanBuildFrom[GenSet[Elem],Elem,Repr[Elem]]) =
    new Aggregator[Repr[Elem],In[Elem]] {
      override def zero: Repr[Elem] = bf().result
      override def insert(s1: Repr[Elem], s2: In[Elem]) =
        (s1.++(s2)(bf))
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

  implicit def GenMapMonoidForMap[Repr[K,V] <: GenMap[K,V], Key, Value, In[X] <: GenTraversableOnce[X], Elem]
                           (implicit bf: CanBuildFrom[Nothing,(Key,Value),Repr[Key,Value]],
                                     va: Aggregator[Repr[Key,Value],(Key,Elem)]) =
    new Aggregator[Repr[Key,Value],In[(Key,Elem)]] {
      override def zero: Repr[Key,Value] = bf().result
      override def insert(m1: Repr[Key,Value], m2: In[(Key,Elem)]) =
       (m1 /: m2)( (m: Repr[Key,Value], e2: (Key,Elem)) => va insert (m,e2) )
    }
  implicit def GenMapMonoidForSeq[Repr[K,V] <: GenMap[K,V], Key, Value, In[X,Y] <: GenMap[X,Y], Elem]
                           (implicit bf: CanBuildFrom[Nothing,(Key,Value),Repr[Key,Value]],
                                     va: Aggregator[Repr[Key,Value],(Key,Elem)]) =
    new Aggregator[Repr[Key,Value],In[Key,Elem]] {
      override def zero: Repr[Key,Value] = bf().result
      override def insert(m1: Repr[Key,Value], m2: In[Key,Elem]) =
       (m1 /: m2)( (m: Repr[Key,Value], e2: (Key,Elem)) => va insert (m,e2) )
    }
  
  implicit def GroupAggregator[Coll,In[X] <: GenTraversableOnce[X],Elem]
                          (implicit va: Aggregator[Coll,Elem])=
    new Aggregator[Coll,In[Elem]] {
	  override def zero = va.zero
	  override def insert(a: Coll, as: In[Elem]) =
	    (a /: as)( (c,e) => va insert (c,e) )
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
  
  implicit def OptionMonoid[A](implicit va: Aggregator[A,A]) =
    new Aggregator[A,Option[A]] {
      override def zero = va.zero
      override def insert(a1: A, a2: Option[A]) = {
        a2 match {
          case None => a1
          case Some(a2) => va insert (a1,a2)
        }
      }
    }
  
  class AggregatorIdentity[A](a: A) {

    def |<|[B](b: B)
              (implicit agg: Aggregator[A,B]) =
      agg insert (a, b)

  }

  implicit def mkAggregator[Coll](c: Coll) = new AggregatorIdentity[Coll](c)

}
