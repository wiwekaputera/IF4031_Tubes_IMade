package hazard

import "testing"

func TestID_DeterministicAndFixedVector(t *testing.T) {
	// sha256("BMKG|evt-1")[:16] = "ae2aa099eacc9511"
	got := ID(SourceBMKG, "evt-1")
	const want = "hz-ae2aa099eacc9511"
	if got != want {
		t.Fatalf("ID(BMKG, evt-1) = %q, want %q", got, want)
	}

	// Calling again produces the exact same ID.
	if again := ID(SourceBMKG, "evt-1"); again != got {
		t.Fatalf("ID not deterministic: %q != %q", again, got)
	}

	// Different source or ref produces a different ID.
	if other := ID(SourcePVMBG, "evt-1"); other == got {
		t.Fatalf("expected different ID for different source, got %q", other)
	}
	if other := ID(SourceBMKG, "evt-2"); other == got {
		t.Fatalf("expected different ID for different source_ref_id, got %q", other)
	}
}

func TestEnumValid(t *testing.T) {
	for _, s := range []Source{SourceBMKG, SourcePVMBG, SourceSatelit, SourceSensorPantai} {
		if !s.Valid() {
			t.Errorf("Source(%q).Valid() = false, want true", s)
		}
	}
	if Source("UNKNOWN").Valid() {
		t.Error("Source(UNKNOWN).Valid() = true, want false")
	}

	for _, ht := range []HazardType{HazardTypeSeismic, HazardTypeVolcanic, HazardTypeWildfire, HazardTypeCoastal} {
		if !ht.Valid() {
			t.Errorf("HazardType(%q).Valid() = false, want true", ht)
		}
	}
	if HazardType("FLOOD").Valid() {
		t.Error("HazardType(FLOOD).Valid() = true, want false")
	}

	for _, sev := range []Severity{SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas} {
		if !sev.Valid() {
			t.Errorf("Severity(%q).Valid() = false, want true", sev)
		}
	}
	if Severity("EXTREME").Valid() {
		t.Error("Severity(EXTREME).Valid() = true, want false")
	}
}
