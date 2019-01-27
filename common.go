package franz

import (
	"github.com/sirupsen/logrus"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func NewLogger(name string) *logrus.Entry {
	log := logrus.New()
	log.SetNoLock()
	return log.WithField("type", name)
}
