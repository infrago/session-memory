package session_memory

import (
	"github.com/infrago/session"
)

func Driver() session.Driver {
	return &memoryDriver{}
}

func init() {
	session.Register("memory", Driver())
}
