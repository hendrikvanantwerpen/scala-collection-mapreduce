================================================
MapReduce with Aggregators for Scala Collections
================================================

This library implements MapReduce for the Scala collections library. The
library uses the idea of aggregators as used in the Google Sawzall
DSL. Specifically the formalization by Lämmel [1] was leading in the
design. This was combined with the generic design of the collections
library and the pimp-my-library pattern to provide seamless integration
and an elegant syntax for the user.

Features
--------

* The use of inferred generic aggregators (reduce) allow the user to
  focus on the data transformations (map). Boilerplate code for handling
  the reduction is reduced. ::

    val wordCount = words.foldLeft(Map.empty[String,Int]) {
      (m,w) => m + (w -> (m.getOrElse(w,0) + 1))
    }
  
  write ::

    val wordCount = words.mapReduce[Map[String,Int]]( w => (w,1) )

* Integrates with the Scala collections and allows mixing of map-reduce
  and standard collection functions in an familiar way. ::
       
    val words:List[String] = ...
    val magicWords = 
      words.filter( _.size > 0 )
           .mapReduce[Map[String,Int]]( w => (w,1) )
           .filter( _._2 == 42 )

* Aggregators can be used stand-alone with the ``|<|`` operator and are
  very liberal in their input. Some examples are ::

    // Int |<| Int : Int
    1 |<| 2 // = 3

    // List[Int] |<| Set[Int] : List[Int]
    List(1,2) |<| Set(3) // = List(1,2,3)

    // SortedSet[Int] |<| Int : SortedSet[Int]
    SortedSet(2,3) |<| 1 // = SortedSet(1,2,3)

    // simple word count coming up!
    // Map[String,Int] |<| (String,Int) : Map[String,Int]
    Map("aap"->1,"noot"->2) |<| ("noot",1) // = Map("aap"->1,"noot"->3)

* Aggregators for Seq, Map and Sets provided with intuitive reduction
  behaviour that fits many uses.

* Collection aggregators are generic and work for all subtypes that
  provide a CanBuildFrom implementation (both sequential and parallel).

* New aggregators are implemented with only a few lines of code.

Build the library
-----------------

The library is currently not published to a public repository, so you'll
have to build the library yourself. This is very easy::

  $ cd scala-collection-mapreduce/
  $ ./sbt
  > + publish-local

The library is currently built for Scala 2.9.{1,2}. If you use a different
version, add your version to ``build.sbt``::

  crossScalaVersions := Seq("2.9.1","2.9.2",YOUR_VERSION)

Usage
-----

To use the library in your own project, add the following dependency to
your ``build.sbt``::

  libraryDependencies += "net.van-antwerpen" %% "scala-collection-mapreduce" % "0.1.0-SNAPSHOT"

In your source include ::

  import net.van_antwerpen.scala.collection.mapreduce.MapReduce._
  import net.van_antwerpen.scala.collection.mapreduce.Aggregators._

Design
------

The design of the library relies on generics, type-inference and the
design of the Scala collections library. For an explanation of the design
we refer to the paper in ``doc/scala-collections-mapreduce.pdf``.

Future
------

* Investigate if we can improve the way the implicits are organized (see [2]).

References
----------

#. Ralf Lämmel. 2007. Google's MapReduce programming model -- Revisited. Sci. Comput. Program. 68, 3 (October 2007), 208-237. DOI=10.1016/j.scico.2007.07.001 http://dx.doi.org/10.1016/j.scico.2007.07.001

#. http://eed3si9n.com/revisiting-implicits-without-import-tax
