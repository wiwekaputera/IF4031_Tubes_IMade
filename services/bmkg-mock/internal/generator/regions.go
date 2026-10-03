package generator

type regionTemplate struct {
	name         string
	baseLat      float64
	baseLon      float64
	coastalZones []string
}

var regionTemplates = []regionTemplate{
	{name: "Sulawesi Tengah (Teluk Tomini)", baseLat: -0.25, baseLon: 120.50, coastalZones: []string{"Parigi Moutong", "Poso"}},
	{name: "Sumatera Barat (Kepulauan Mentawai)", baseLat: -2.10, baseLon: 99.60, coastalZones: []string{"Tuapejat", "Sikakap"}},
	{name: "Aceh (Barat Daya)", baseLat: 3.80, baseLon: 95.90, coastalZones: []string{"Meulaboh", "Calang"}},
	{name: "Jawa Barat (Pesisir Selatan Sukabumi)", baseLat: -7.30, baseLon: 106.50, coastalZones: []string{"Palabuhanratu", "Ujung Genteng"}},
	{name: "Nusa Tenggara Barat (Lombok Utara)", baseLat: -8.30, baseLon: 116.30, coastalZones: []string{"Pemenang", "Bayan"}},
	{name: "Maluku (Laut Banda)", baseLat: -4.50, baseLon: 129.90, coastalZones: []string{"Banda Neira", "Amahai"}},
	{name: "Papua (Teluk Cenderawasih)", baseLat: -2.80, baseLon: 134.50, coastalZones: []string{"Nabire", "Wasior"}},
	{name: "Sulawesi Utara (Laut Maluku)", baseLat: 1.50, baseLon: 126.20, coastalZones: []string{"Bitung", "Likupang"}},
	{name: "Bengkulu (Enggaro)", baseLat: -5.30, baseLon: 102.20, coastalZones: []string{"Pulau Enggano", "Manna"}},
	{name: "Bali (Selatan Kuta)", baseLat: -9.10, baseLon: 115.10, coastalZones: []string{"Jimbaran", "Nusa Dua"}},
}
