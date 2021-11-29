object NGramConfig {
  final val MinNumTokens = 3
  final val UnknownToken = "<unk>"
  final val StartToken = "<s>"
  final val EndToken = "<e>"
  final val K = 1.0
  final val NumHints = 5
  final val WordPattern = "\\s+.+\\s+"
  final val WordReplacement = " # "
}
