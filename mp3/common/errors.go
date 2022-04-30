package common

import "errors"

var ErrorNotFound = errors.New("acct not found")
var ErrorIsolationViolated = errors.New("isolation violated")