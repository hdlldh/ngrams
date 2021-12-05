object Config {
  final val MinNumTokens = 3
  final val UnknownToken = "<unk>"
  final val StartToken = "<sos>"
  final val EndToken = "<eos>"
  final val K = 1.0
  final val NumHints = 5
  final val N = 3
  final val NumStartTokens = 1
  final val NumEndTokens = 1
  final val WordExtractPattern = (1 to N).map(_=>"(\\S+)").mkString(" ")
  final val CenterWordIndex = 2
  final val WordReplacement = (1 to N).map {n=>
    if (n == CenterWordIndex) "#"
    else s"$n"
  }.mkString(" ")
}
