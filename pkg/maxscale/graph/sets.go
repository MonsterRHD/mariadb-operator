package graph

import (
	"sort"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

type IDSet map[string]struct{}

func newIDSet(ids ...string) IDSet {
	set := make(IDSet, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func (s IDSet) Add(id string) {
	s[id] = struct{}{}
}

// Remove deletes an ID from the set.
func (s IDSet) Remove(id string) {
	delete(s, id)
}

func (s IDSet) Contains(id string) bool {
	_, ok := s[id]
	return ok
}

func (s IDSet) Sorted() []string {
	ids := make([]string, 0, len(s))
	for id := range s {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s IDSet) Union(other IDSet) IDSet {
	merged := make(IDSet, len(s)+len(other))
	for id := range s {
		merged[id] = struct{}{}
	}
	for id := range other {
		merged[id] = struct{}{}
	}
	return merged
}

func (s IDSet) Intersect(other IDSet) IDSet {
	result := make(IDSet)
	for id := range s {
		if other.Contains(id) {
			result.Add(id)
		}
	}
	return result
}

// minus returns the IDs present in s but not in other.
func (s IDSet) Minus(other IDSet) IDSet {
	result := make(IDSet)
	for id := range s {
		if !other.Contains(id) {
			result.Add(id)
		}
	}
	return result
}

func (s IDSet) Equal(other IDSet) bool {
	if len(s) != len(other) {
		return false
	}
	for id := range s {
		if !other.Contains(id) {
			return false
		}
	}
	return true
}

// containsOtherThan reports whether the set has any ID different from the given one.
func (s IDSet) ContainsOtherThan(id string) bool {
	for current := range s {
		if current != id {
			return true
		}
	}
	return false
}

// SameStrings reports whether two slices contain the same string set, ignoring order and duplicates.
func SameStrings(a, b []string) bool {
	return setsFromIDs(a).Equal(setsFromIDs(b))
}

func setsFromIDs(ids []string) IDSet {
	return newIDSet(ids...)
}

func idsFromSets(servers, filters, monitors, services, listeners IDSet) *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return &mariadbv1alpha1.MaxScaleConfigObjectIDs{
		Servers:   servers.Sorted(),
		Filters:   filters.Sorted(),
		Monitor:   monitors.Sorted(),
		Services:  services.Sorted(),
		Listeners: listeners.Sorted(),
	}
}

func SortedStrings(ids []string) []string {
	sort.Strings(ids)
	return ids
}
