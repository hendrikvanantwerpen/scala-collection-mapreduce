# MapReduce for Scala Collections

This library implements MapReduce using generalized Monoids as described
in [1]. The library takes a pimp-my-library approach to integrate
seamlessly with the Scala Collections library.

## Features

 * Seamless integration with Scala collections
 * Works on both sequential and parallel collections
 * Use all Monoids from Scalaz library
 * General aggregators defined for Seq, Map, Set and Tuple
   and any of their subtypes.

The text below decribed my own process of developing the Monoid
extensions. Since the main idea is described in the paper, I'll rewrite
this to describe the implementation details only.

## Reduction using Monoids

The problem is that they only combine monoids. If our monoids are
collections, like sets or maps, a new set is created for every map
operation. In our case often every mapreduced element corresponded to one
new element. This caused the creation of a lot of element, resulting in
very bad performance. Not only was performance bad, it got worse with
every next mapReduce, suggesting the the JVM was going down. It seems
other people have observed the same behaviour with lots of short-lived
objects are involved: http://stackoverflow.com/a/2189532

To compare: a handwritten version using folds and maps over Scala's
collections worked orders faster. The fact that mapping and composition
logic was mixed was obviously not nice. Also the combination logic got
more complicated with every level that was added and the combination
logic was reproduced in every fold for the same kind of collections.

## Reduce elements to collections

Can we have the simplicity of a monoid, but append elements to
collections: multoids? First attempt was for maps with monoid values. It
works but has limitations. Different functions for monoid or multoid
values in the map. Inference only one level deep, then we need to build
them explicitly. Also the monoids are for concrete types only.

## Generic collection accumulators

We want the multoids to work with any concrete map type without having
to implement them over and over again. Leverage the design of the Scala
collection library. We use generic builders that can be implicitly looked
up to create a collection of the correct specific type. We are able to
infer the multoids over maps and collections, but still only one level
deep. Also the difference between multoid and monoid values in maps is
still there.

Note that using the CanBuildFrom approach, the Multoids only work for
immutable collections. (is this true??)

Performance was bad when we used to builder, because our function is
called for every element, but then rebuild the collection using the
builder. Vast improvement when we used the builder only for the empty
collection and then appended elements using the normal + operators. These
messed up the types a bit, giving only a base type, so we cast to the
correct type.

## Fully inferred generic accumulators

Unified the cases for monoids and multoids by implicitly wrapping monoids
in multoids who have the same collection as element type.

Reformulated the types of the implicit functions so we can access the
type parameters of the collections we get (e.g. the key and value type of
map). This allowed us to completely infer multoids that go several levels
deep. We created general multoids for maps, sets, sequences and tuples.

There are situations where conflicts can occur. E.g. the Tuple2Multoid and
the Tuple2Monoid both match when the tuple values are Monoids. Haven't found
a way to resolve this automatically yet. You'll have to pick one by hand. 

Performance of the final solution is very close to using handwritten folds.

## Integrating MapReduce by pimping Scala Collections

By using a simple implicit conversion we can add mapReduce and
flatMapReduce functions to all Traversable types. This was we can
chain mapReduce calls but also mix them with the regular collection
operations. By doing this we can skip one type parameter on mapReduce
because it can be inferred from the collection it is invoked on.

By making mapReduce a function without arguments but one type parameter
that returns an object with an apply method, we can make our code even
DRY-er.  We only have to specify the expected collection, the element
type is inferred from the map function we pass.

Finally got builders to work with proper speed, don't know what went
wrong before. No ugly casts anymore and only Map is a special case.

## Comparison to traditional MapReduce and Monoidic MapReduce

 * Monoids
   - reduce boilerplate
   - works well if individual items reduce to rather big data structures
 * Map & Reduce
   - more boilerplate, nested reduction gets nasty
   - still no strong key dependency
 * Multoids
   - Similar to Monoids in boiler plate
   - Include all available Monoids
   - Allow append of elements, not just collections which makes it much
     better fit for listitem to listitem operations where the dataset is
     transformed but the size stays the same.
   
## Build & Test

```sh
$ cd scala-collection-mapreduce
$ sbt
> test
```

## Related

 * http://www.cs.berkeley.edu/~matei/spark/
   - Seems to do only (traditional) map/reduce seperatly
 * http://hackage.haskell.org/packages/archive/monoids/0.2.0.4/doc/html/src/Data-Monoid-Reducer.html
   - This looks like an implemenation of the idea in Haskell
 * Scalaz Reducers?
   - This is an implementation of the idea in Scalaz
   - How integrated is it with the collections library?
   - Only seem to support List and Stream concrete types
   - Do not seem to support parallel collections
 * https://github.com/scala-incubator/distributed-collections

## Notes & ideas

 * Scalaz dependency:
   - Will users need to pull in scalaz or the accumulators themselves?
     It seems so, can we carry them along with MapReduce automagically?
   - Will users be repulsed by the scalaz dependency?
 * Do we want to resolve implicits on every level? E.g.
   ```scala
   (Int,Map[String,Int]) |<| (Int,(Int,Int)) // convert second Int implicitly to String
   (Int,Map[String,String]) |<| (Int,(Int,Int)) // convert second and third Int implicitly to String
   ```
 * Is the current approach with invariant type parameters enough?
   This seems mainly a problem on the Monoid level ( SortedMap[K,V] |+| Map[K,V] breaks, but could work )
 * What are the consequences of having every Monoid <: Aggregator
   vs. Laemmels approach of every Aggregator <: Monoid
   - Making Aggregator a Monoid and implicitly convert Monoids to
     Aggregators causes some weird errors. The latter is needed because
     we cannot take a Monoid as the general function argument. We wouldn't
     have enough type parameters to infer the aggregator if needed.

## References

 1. Ralf Lämmel. 2007. Google's MapReduce programming model -- Revisited. Sci. Comput. Program. 68, 3 (October 2007), 208-237. DOI=10.1016/j.scico.2007.07.001 http://dx.doi.org/10.1016/j.scico.2007.07.001
