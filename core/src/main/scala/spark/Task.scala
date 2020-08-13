package spark

class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Int) extends Serializable

abstract class Task[T] extends Serializable {
  def run(id: Int): T // Note: Critical method, run the submitted task.
  def preferredLocations: Seq[String] = Nil // Note: This is use to do a best effort local compute.
  def generation: Option[Long] = None // Note: Generation is the times that task fails due to exception like fetch failure.
}
