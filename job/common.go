package job

func normalizeExitCode(code int) int {
	if code > 0 {
		return 1
	}
	return 0
}
