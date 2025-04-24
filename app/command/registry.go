package command

import "net"

var countAcknowledgingReplicas func(int) int
var waitForAcknowledgment func()
var getClientOffset func(net.Conn) (int, error)

func RegisterCountAcknowledgingReplicas(fn func(int) int) {
	countAcknowledgingReplicas = fn
}

func RegisterWaitForAcknowledgment(fn func()) {
	waitForAcknowledgment = fn
}

func RegisterGetClientOffset(fn func(net.Conn) (int, error)) {
	getClientOffset = fn
}
