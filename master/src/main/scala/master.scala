package master

class Key(protected val key: Int) {
  def <(that: Key): Boolean = key < that.key
  def ==(that: Key): Boolean = key == that.key
}

object Key {
  val MAX: Key = new Key(Int.MaxValue)
  val MIN: Key = new Key(Int.MinValue)
}

object Master {
  def divideKeyRange(
    populationSize: Int,
    numSamples: Int,
    samples: Iterable[Iterable[Byte]],
    ): (String, (List[Byte], List[Byte])) = {
    ("", (List(), List()))
  }
}

class Master {}
