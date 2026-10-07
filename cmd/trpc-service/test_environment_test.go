package main

func mapEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func cloneEnvironment(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
