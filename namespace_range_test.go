package main

import (
	"fmt"
	"testing"

	"math/big"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

type (
	// a test model used to populate namespaces
	pet struct {
		Value int64 `datastore:"value"`
	}
)

func createInNamespace(c context.Context, namespace string) {
	ns, _ := appengine.Namespace(c, namespace)
	p := new(pet)
	p.Value = 1
	k := datastore.NewKey(ns, "pet", "", 1, nil)
	datastore.Put(ns, k, p)
}

func TestOrdinalization(t *testing.T) {
	setupConstants("ab", 2, 0)
	tests := []struct {
		ordinal   *big.Int
		namespace string
	} {
		{big.NewInt(0), ""},
		{big.NewInt(1), "a"},
		{big.NewInt(2), "aa"},
		{big.NewInt(3), "ab"},
		{big.NewInt(4), "b"},
		{big.NewInt(5), "ba"},
		{big.NewInt(6), "bb"},
	}
	for i, test := range tests {
		if ns := ordToNamespace(test.ordinal, 0); ns != test.namespace {
			t.Errorf("%d ordToNamespace %s failed - expected %s got %s", i, test.ordinal, test.namespace, ns)
		}

		if ord := namespaceToOrd(test.namespace); ord.Cmp(test.ordinal) != 0 {
			t.Errorf("%d namespaceToOrd %s failed - expected %s got %s", i, test.namespace, test.ordinal, ord)
		}
	}
}

func TestNamespaceRangeIteration(t *testing.T) {
	setupConstants("abc", 3, 3)
}

func TestNamespaceRangeSplit(t *testing.T) {
	setupConstants("abc", 3, 3)
}

func TestNamespaceSplit(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	for _, x := range "cba" {
		for _, y := range "abc" {
			for _, z := range "bac" {
				v := fmt.Sprintf("%c%c%c", x, y, z)
				createInNamespace(c, v)
			}
		}
	}

	tests := []struct {
		name       string
		count      int
		contiguous bool
		canQuery   bool
		ranges     []NamespaceRange
	} {
		{"testSplitWithoutQueries", 4, false, false, []NamespaceRange{
				{Start:"", End:"abc"},
				{Start:"ac", End:"bb"},
				{Start:"bba", End:"caa"},
				{Start:"cab", End:"ccc"},
			},
		},
		{"testSplitWithoutQueriesWithContiguous", 4, true, false, []NamespaceRange{
				{Start:"", End:"abc"},
				{Start:"ac", End:"bb"},
				{Start:"bba", End:"caa"},
				{Start:"cab", End:"ccc"},
			},
		},
	}
	for _, test := range tests {
		results, _ := namespaceSplit(c, test.count, test.contiguous, test.canQuery)
		if len(results) != len(test.ranges) {
			t.Fatalf("%s expected %d ranges got %d", test.name, len(test.ranges), len(results))
		}
		for i, r := range results {
			if r.Start != test.ranges[i].Start || r.End != test.ranges[i].End {
				t.Fatalf("%s expected range %d %s-%s got %s-%s", test.name, i, test.ranges[i].Start, test.ranges[i].End, r.Start, r.End)
			}
		}
	}
}

func TestNone(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	tests := []struct {
		name       string
		count      int
		contiguous bool
		canQuery   bool
		ranges     []NamespaceRange
	} {
		{"testSplitWithNoNamespacesInDatastore", 10, false, true, []NamespaceRange{},},
	}
	for _, test := range tests {
		results, _ := namespaceSplit(c, test.count, test.contiguous, test.canQuery)
		if len(results) != len(test.ranges) {
			t.Fatalf("%s expected %d ranges got %d", test.name, len(test.ranges), len(results))
		}
		for i, r := range results {
			if r.Start != test.ranges[i].Start || r.End != test.ranges[i].End {
				t.Fatalf("%s expected range %d %s-%s got %s-%s", test.name, i, test.ranges[i].Start, test.ranges[i].End, r.Start, r.End)
			}
		}
	}
}

