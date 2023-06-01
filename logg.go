package walrus

import "log"

// Set this to true to enable logging
var Logging bool

func logg(fmt string, args ...interface{}) {
	if Logging {
		log.Printf("Walrus: "+fmt, args...)
	}
}
