package spark

class Aggregator[K, V, C] ( // Note: K is key's type, V is value's type, C is how we accumulate value by key. For example, C and V is the same for reduceByKey, see PairRDDFunctions.reduceByKey
    val createCombiner: V => C, // Note: Create a combiner for value.
    val mergeValue: (C, V) => C, // Note: Merge value to combiner.
    val mergeCombiners: (C, C) => C) // Note: Usually we can infer mergeCombiners from V and C's type. And in practice, usually use define createCombiner and mergeValue is enough.
  extends Serializable
