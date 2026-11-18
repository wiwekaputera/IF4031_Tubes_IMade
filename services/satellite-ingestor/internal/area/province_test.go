package area

import "testing"

func TestTableHasAllProvinces(t *testing.T) {
	if len(Provinces) != 38 {
		t.Fatalf("table has %d provinces, want 38", len(Provinces))
	}
	seen := map[string]bool{}
	for _, p := range Provinces {
		if seen[p.Name] {
			t.Fatalf("duplicate province %q", p.Name)
		}
		seen[p.Name] = true
	}
}

func TestNearest(t *testing.T) {
	cases := []struct {
		name     string
		lat, lon float64
		want     string
	}{
		{"fire cluster seen in the live feed", -1.98905, 112.64071, "Kalimantan Tengah"},
		{"peatland near Pekanbaru", 0.6, 101.6, "Riau"},
		{"south Sumatra", -3.2, 104.9, "Sumatera Selatan"},
		{"central Sulawesi", -0.9, 119.9, "Sulawesi Tengah"},
		{"exactly on a capital", -6.2088, 106.8456, "DKI Jakarta"},
	}
	for _, c := range cases {
		if got := Nearest(c.lat, c.lon).Name; got != c.want {
			t.Errorf("%s: Nearest(%v, %v) = %q, want %q", c.name, c.lat, c.lon, got, c.want)
		}
	}
}
