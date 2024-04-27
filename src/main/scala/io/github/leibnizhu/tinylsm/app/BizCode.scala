package io.github.leibnizhu.tinylsm.app

enum BizCode(val code: Int) {
  case Success extends BizCode(10000)
  case KeyNotExists extends BizCode(20001)
  case TransactionNotExists extends BizCode(20002)
  case TransactionInvalid extends BizCode(20003)
  case TransactionCommitFailed extends BizCode(20004)
  case CommonError extends BizCode(99999)
}
