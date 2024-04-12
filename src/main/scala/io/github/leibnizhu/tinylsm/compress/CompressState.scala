package io.github.leibnizhu.tinylsm.compress

enum CompressState {
  case Train extends CompressState
  case Compress extends CompressState
  case Decompress extends CompressState
}