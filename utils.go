package main

func inList(needle string, haystack []string) bool {
	for _, i := range haystack {
		if needle == i {
			return true
		}
	}
	return false
}
