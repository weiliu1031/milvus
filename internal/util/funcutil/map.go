package funcutil

import "fmt"

func MapReduce(results []map[string]string, method map[string]func(string) error) error {
	// TODO: use generic type to reconstruct map[string]string -> [T any] map[string]T
	for _, result := range results {
		for k, v := range result {
			fn, ok := method[k]
			if !ok {
				return fmt.Errorf("unknown field %s", k)
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func Extract[K comparable, V any](data map[K]V, keys ...K) []V {
	slice := make([]V, 0, len(keys))
	for _, key := range keys {
		slice = append(slice, data[key])
	}
	return slice
}
