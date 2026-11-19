// Package area names the place a fire is in. It uses a static table of the
// 38 Indonesian provinces, each represented by the coordinates of its capital,
// and picks the province whose capital is closest to the fire's centre.
//
// Using capitals is an approximation: a fire near a provincial border can be
// assigned to the neighbouring province, and a fire outside Indonesia gets the
// closest Indonesian province. Polygon boundaries would fix both, at the cost of
// shipping boundary data.
package area

import "math"

// Province is one entry of the reference table.
type Province struct {
	Name     string
	Capital  string
	Lat, Lon float64
}

// Provinces is the static reference table, west to east.
var Provinces = []Province{
	{"Aceh", "Banda Aceh", 5.5483, 95.3238},
	{"Sumatera Utara", "Medan", 3.5952, 98.6722},
	{"Sumatera Barat", "Padang", -0.9471, 100.4172},
	{"Riau", "Pekanbaru", 0.5071, 101.4478},
	{"Kepulauan Riau", "Tanjung Pinang", 0.9186, 104.4554},
	{"Jambi", "Jambi", -1.6101, 103.6131},
	{"Sumatera Selatan", "Palembang", -2.9761, 104.7754},
	{"Kepulauan Bangka Belitung", "Pangkal Pinang", -2.1291, 106.1090},
	{"Bengkulu", "Bengkulu", -3.7928, 102.2608},
	{"Lampung", "Bandar Lampung", -5.3971, 105.2668},
	{"DKI Jakarta", "Jakarta", -6.2088, 106.8456},
	{"Banten", "Serang", -6.1200, 106.1503},
	{"Jawa Barat", "Bandung", -6.9175, 107.6191},
	{"Jawa Tengah", "Semarang", -6.9667, 110.4167},
	{"DI Yogyakarta", "Yogyakarta", -7.7956, 110.3695},
	{"Jawa Timur", "Surabaya", -7.2575, 112.7521},
	{"Bali", "Denpasar", -8.6705, 115.2126},
	{"Nusa Tenggara Barat", "Mataram", -8.5833, 116.1167},
	{"Nusa Tenggara Timur", "Kupang", -10.1772, 123.6070},
	{"Kalimantan Barat", "Pontianak", -0.0263, 109.3425},
	{"Kalimantan Tengah", "Palangka Raya", -2.2136, 113.9108},
	{"Kalimantan Selatan", "Banjarbaru", -3.4572, 114.8103},
	{"Kalimantan Timur", "Samarinda", -0.5022, 117.1536},
	{"Kalimantan Utara", "Tanjung Selor", 2.8375, 117.3653},
	{"Sulawesi Utara", "Manado", 1.4748, 124.8421},
	{"Gorontalo", "Gorontalo", 0.5435, 123.0568},
	{"Sulawesi Tengah", "Palu", -0.8917, 119.8707},
	{"Sulawesi Barat", "Mamuju", -2.6748, 118.8885},
	{"Sulawesi Selatan", "Makassar", -5.1477, 119.4327},
	{"Sulawesi Tenggara", "Kendari", -3.9985, 122.5129},
	{"Maluku", "Ambon", -3.6954, 128.1814},
	{"Maluku Utara", "Sofifi", 0.7372, 127.5588},
	{"Papua Barat Daya", "Sorong", -0.8762, 131.2558},
	{"Papua Barat", "Manokwari", -0.8615, 134.0620},
	{"Papua Tengah", "Nabire", -3.3667, 135.4833},
	{"Papua Pegunungan", "Wamena", -4.0956, 138.9449},
	{"Papua", "Jayapura", -2.5337, 140.7181},
	{"Papua Selatan", "Merauke", -8.4932, 140.4018},
}

// Nearest returns the province whose capital is closest to (lat, lon).
func Nearest(lat, lon float64) Province {
	best, bestKm := Provinces[0], math.Inf(1)
	for _, p := range Provinces {
		if d := distanceKm(lat, lon, p.Lat, p.Lon); d < bestKm {
			best, bestKm = p, d
		}
	}
	return best
}

// distanceKm is the haversine distance on a 6371 km sphere.
func distanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.0
	toRad := math.Pi / 180
	dLat := (lat2 - lat1) * toRad
	dLon := (lon2 - lon1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*toRad)*math.Cos(lat2*toRad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * r * math.Asin(math.Sqrt(a))
}
