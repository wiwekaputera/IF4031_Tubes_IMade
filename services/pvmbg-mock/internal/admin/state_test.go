package admin

import "testing"

func TestIncompatibleToggleIsIndependentOfAdditiveToggle(t *testing.T) {
	s := NewSchemaVersionState()
	if !s.ToggleIncompatible() || !s.IsIncompatible() {
		t.Fatal("incompatible mode should turn on")
	}
	if s.IsEnabled() {
		t.Fatal("additive confidence_level mode must not change")
	}
	if s.ToggleIncompatible() || s.IsIncompatible() {
		t.Fatal("incompatible mode should turn off on second toggle")
	}
}
