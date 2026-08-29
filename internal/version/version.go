package version

var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

func Info() string {
	return "takl " + Version + " (" + GitCommit + ") built " + BuildDate
}
