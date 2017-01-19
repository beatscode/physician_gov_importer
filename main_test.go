package main

import (
	_ "fmt"
	"testing"
)

func TestProperCapitalization(t *testing.T) {

	s := "AT THE REPUBLIC AND STUFF FOR United STATES OF TO TIME"

	CapitalizeTitle(&s)

	if s != "At the Republic and Stuff for United States of to Time" {
		t.Error("invalid capitalization path: \n", s)
	}
}
