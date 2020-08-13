package spark

class ResultTask[T, U](
    runId: Int,
    stageId: Int, 
    rdd: RDD[T], 
    func: (TaskContext, Iterator[T]) => U,
    val partition: Int, 
    locs: Seq[String],
    val outputId: Int)
  extends DAGTask[U](runId, stageId) {
  
  val split = rdd.splits(partition)
  // Note: The rdd.iterator call will call the compute function of rdd, the compute's behaviour is different in different type of RDD. for MappedRDD, it will call prev.iterator(split).map(f), in any case this will trigger a recursive calling of iterator and compute funcition.
  override def run(attemptId: Int): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, rdd.iterator(split)) // Note: func is of type (TaskContext, Iterator[T]) => U, it will turn Iterator[T] -> U, it is an action.
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
