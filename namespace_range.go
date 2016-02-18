package main

import (
	"fmt"
	"strings"
	"sort"

	"math/big"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

const namespaceKind = "__namespace__"

type (
	// NamespaceRange represents a namespace range
	NamespaceRange struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}

	byStart []*NamespaceRange
)

var (
	lexDistance []*big.Int

	namespaceCharacters string
	maxNamespaceLength int
	namespaceBatchSize int
	minNamespace string
	maxNamespace string
)

func init() {
	setupConstants("-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz", 100, 50)
}

func setupConstants(alphabet string, maxLength, batchSize int) {
	namespaceCharacters = alphabet
	maxNamespaceLength = maxLength
	minNamespace = ""
	maxNamespace = strings.Repeat(alphabet[len(alphabet)-1:], maxLength)
	namespaceBatchSize = batchSize

	lexDistance = make([]*big.Int, maxLength)
	lexDistance[0] = big.NewInt(1)
	length := big.NewInt(int64(len(alphabet)))

	for i := 1; i < maxLength; i++ {
		temp := new(big.Int)
		temp.Mul(lexDistance[i-1], length)
		temp.Add(temp, big.NewInt(1))
		lexDistance[i] = temp
	}
}

func newNamespaceRange(start, end string) *NamespaceRange {
	if start == "" {
		start = minNamespace
	}
	if end == "" {
		end = maxNamespace
	}
	if start > end {
		// error
	}
	return &NamespaceRange{
		Start: start,
		End: end,
	}
}

func (s byStart) Len() int           { return len(s) }
func (s byStart) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byStart) Less(i, j int) bool { return s[i].Start < s[j].Start }

// IsSingleNamespace returns True if the namespace range only includes a single namespace
func (n *NamespaceRange) IsSingleNamespace() bool {
	return n.Start == n.End
}

// Split splits the NamespaceRange into two nearly equal-sized ranges
// If this NamespaceRange contains a single namespace then a list containing
// this NamespaceRange is returned. Otherwise a two-element list containing
// two NamespaceRanges whose total range is identical to this
// NamespaceRange's is returned.
func (n *NamespaceRange) Split() (*NamespaceRange, *NamespaceRange) {
	if n.IsSingleNamespace() {
		return n, nil
	}
	midPoint := new(big.Int)
	midPoint.Add(namespaceToOrd(n.Start), namespaceToOrd(n.End))
	midPoint.Div(midPoint, big.NewInt(2))

	left := newNamespaceRange(n.Start, ordToNamespace(midPoint, 0))
	right := newNamespaceRange(ordToNamespace(midPoint.Add(midPoint, big.NewInt(1)), 0), n.End)

	return left, right
}

// WithStartAfter returns a copy of this NamespaceRange with a new start
func (n *NamespaceRange) WithStartAfter(afterNamespace string) *NamespaceRange {
	temp := new(big.Int)
	namespaceStart := ordToNamespace(temp.Add(namespaceToOrd(afterNamespace), big.NewInt(1)), 0)
  return newNamespaceRange(namespaceStart, n.End)
}

// MakeDatastoreQuery returns a datastore.Query that generates all namespaces in the range
func (n *NamespaceRange) MakeDatastoreQuery(c context.Context, start string) *datastore.Query {
	q := datastore.NewQuery(namespaceKind)
	if n.Start != "" {
		q = q.Filter("__key__ >=", datastore.NewKey(c, namespaceKind, n.Start, 0, nil))
	}
	q = q.Filter("__key__ <=", datastore.NewKey(c, namespaceKind, n.End, 0, nil))
	q = q.Order("__key__")
	q = q.KeysOnly()
	if start != "" {
		cursor, _ := datastore.DecodeCursor(start)
		q = q.Start(cursor)
	}
	return q
}

// NormalizedStart returns a NamespaceRange with leading non-existant namespaces removed
// A copy of this NamespaceRange whose namespace_start is adjusted to exclude
// the portion of the range that contains no actual namespaces in the
// datastore. None is returned if the NamespaceRange contains no actual
// namespaces in the datastore.
func (n *NamespaceRange) NormalizedStart(c context.Context) *NamespaceRange {
	q := n.MakeDatastoreQuery(c, "")
	namespaceAfterKey, _ := q.Limit(1).GetAll(c, nil)
	// fmt.Printf("NormalizedStart %s %#v\n", n.Start, namespaceAfterKey)
	if len(namespaceAfterKey) == 0 {
		return nil
	}
	return newNamespaceRange(namespaceAfterKey[0].Namespace(), n.End)
}

// Convert a namespace ordinal to a namespace string
func ordToNamespace(n *big.Int, maxLength int) string {
	if n.Int64() == 0 {
		return ""
	}

	if maxLength == 0 {
		maxLength = maxNamespaceLength
	}
	maxLength--
	length := lexDistance[maxLength]
	tmp := new(big.Int)
	tmp.Sub(n, big.NewInt(1))
	index := new(big.Int)
	index.Div(tmp, length)
	mod := new(big.Int)
	mod.Mod(tmp, length)

  return namespaceCharacters[index.Int64():index.Int64() + 1] + ordToNamespace(mod, maxLength)
}

// Converts a namespace string into an int representing its lexographic order
func namespaceToOrd(namespace string) *big.Int {
	n := new(big.Int)
	for i, c := range namespace {
		pos := strings.IndexRune(namespaceCharacters, c)
		tmp := new(big.Int)
		ld := lexDistance[maxNamespaceLength - i - 1]
		tmp.Mul(ld, big.NewInt(int64(pos)))
		n.Add(n, tmp)
		n.Add(n, big.NewInt(1))
	}
	return n
}

func getNamespaces(c context.Context, limit int) ([]string, error) {
	q := datastore.NewQuery(namespaceKind).Limit(limit).KeysOnly()
	keys, err := q.GetAll(c, nil)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(keys))
	for _, k := range keys {
		names = append(names, k.StringID())
	}
	return names, nil
}

// Splits the complete NamespaceRange into n equally-sized NamespaceRanges.
func namespaceSplit(c context.Context, n int, contiguous, canQuery bool) ([]*NamespaceRange, error) {
	if n < 1 {
		return nil, fmt.Errorf("n must be >= 1")
	}

	nsRanges := []*NamespaceRange{}
	if canQuery {
		if contiguous {
			nsRange := newNamespaceRange("", "")
			nsRange = nsRange.NormalizedStart(c)
			nsRanges = append(nsRanges, nsRange)
		} else {
			namespaces, err := getNamespaces(c, n + 1)
			// fmt.Println(namespaces)
			if err != nil || len(namespaces) == 0 {
				return nsRanges, nil
			}
			if len(namespaces) < n {
				// If we have less actual namespaces than number of NamespaceRanges
				// to return, then just return the list of those namespaces.
				for _, ns := range namespaces {
					nsRanges = append(nsRanges, newNamespaceRange(ns, ns))
				}
				sort.Sort(byStart(nsRanges))
				return nsRanges, nil
			}
			nsRanges = append(nsRanges, newNamespaceRange(namespaces[0], ""))
		}
	} else {
		nsRanges = append(nsRanges, newNamespaceRange("", ""))
	}

	//for _, nsRange := range nsRanges {
	//	fmt.Printf("start '%s' end '%s'\n", nsRange.Start, nsRange.End)
	//}

	singles := []*NamespaceRange{}
	for len(nsRanges) > 0 && (len(nsRanges) + len(singles) < n) {
		// fmt.Printf("\nranges: %d\n", len(nsRanges))
		// for _, nsRange := range nsRanges {
		// 	fmt.Printf("%#v\n", nsRange)
		// }
		nsRange := nsRanges[0]
		nsRanges = append(nsRanges[:0], nsRanges[1:]...)
		// nsRanges = nsRanges[1:len(nsRanges)]
		// nsRanges, nsRanges[len(nsRanges)-1] = nsRanges[1:], nil

		if nsRange.IsSingleNamespace() {
			singles = append(singles, nsRange)
		} else {
			left, right := nsRange.Split()
			// fmt.Printf("\nleft %#v\nright %#v\n", left, right)
			if right != nil {
				if canQuery {
					right = right.NormalizedStart(c)
				}
				if right != nil {
					nsRanges = append(nsRanges, right)
				}
			}
			nsRanges = append(nsRanges, left)
		}
	}

	nsRanges = append(nsRanges, singles...)
	sort.Sort(byStart(nsRanges))
	// fmt.Printf("\nsorted: %d\n", len(nsRanges))
	// for _, nsRange := range nsRanges {
	// 	fmt.Printf("%#v\n", nsRange)
	// }

	if contiguous {
		if len(nsRanges) == 0 {
			// This condition is possible if every namespace was deleted after the
			// first call to ns_range.normalized_start().
			nsRanges = []*NamespaceRange{newNamespaceRange("", "")}
			return nsRanges, nil
		}

		continuousRanges := []*NamespaceRange{}
		for i := 0; i < len(nsRanges); i++ {
			// fmt.Printf("\ncontinuous: %d\n", len(nsRanges))
			// for _, nsRange := range nsRanges {
			// 	fmt.Printf("%#v\n", nsRange)
			// }
			var start string
			if i == 0 {
				start = minNamespace
			} else {
				start = nsRanges[i].Start
			}

			var end string
			if i == len(nsRanges) - 1 {
				end = maxNamespace
			} else {
				tmp := new(big.Int)
				tmp.Sub(namespaceToOrd(nsRanges[i+1].Start), big.NewInt(1))
				end = ordToNamespace(tmp, 0)
			}
			// fmt.Printf("start %s end %s\n", start, end)
			continuousRanges = append(continuousRanges, newNamespaceRange(start, end))
		}
		return continuousRanges, nil
	}

	return nsRanges, nil
}
