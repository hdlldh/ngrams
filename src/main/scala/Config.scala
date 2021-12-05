object Config {
  final val MinNumTokens = 3
  final val UnknownToken = "<unk>"
  final val StartToken = "<sos>"
  final val EndToken = "<eos>"
  final val K = 1.0
  final val NumHints = 5
  final val N = 4
  final val NumStartTokens = 2
  final val NumEndTokens = 1
  final val CenterWordIndex = 3
  final val WordExtractPattern = (1 to N).map(_=>"(\\S+)").mkString(" ")
  final val WordReplacement = (1 to N).map {n=>
    if (n == CenterWordIndex) "#"
    else "$"+n.toString
  }.mkString(" ")
}