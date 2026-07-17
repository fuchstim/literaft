package version

var Version string
var Commit string
var Date string

func init() {
	if Version == "" {
		Version = "dev"
	}
	if Commit == "" {
		Commit = "none"
	}
	if Date == "" {
		Date = "unknown"
	}
}
