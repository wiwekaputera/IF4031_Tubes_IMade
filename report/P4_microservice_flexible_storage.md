# P4 — Batas Service & Penyimpanan Fleksibel

Dokumen ini menjelaskan batasan (boundary) antar-container pada sistem BNPB
Disaster Coordination, dan mengapa PostgreSQL JSONB dipilih sebagai penyimpanan
kanonisal dibanding MongoDB atau Cassandra. Penutup dokumen adalah bukti
isolasi restart proses (A7).

---

## Rancangan

### Batas service (container)

Sistem terdiri dari 11 container yang dideklarasikan di
`docker-compose.yml`, masing-masing memiliki kepemilikan data (storage
ownership) yang eksplisit. Jaringan ter-segmentasi
(`store_net`, `token_net`, `internal_api_net`, `bus_net`, `agency_net`,
`edge_net`) memaksa batasan ini pada level jaringan — lihat
`docker-compose.yml:10-26` dan `ARCHITECTURE.md:23-35`.

| Container | Port host | Jaringan | Kepemilikan data / tanggung jawab | Sumber |
| :--- | :--- | :--- | :--- | :--- |
| `bmkg-mock` | 8081 | `agency_net` | Simulasi BMKG. Seed + generator `SeismicEvent`/`TsunamiWarning`. Auth via `X-BMKG-Key`. | `docker-compose.yml:45-65`; `ARCHITECTURE.md:11` |
| `pvmbg-mock` | 8082 | `agency_net` | Simulasi PVMBG. Seed + generator `VolcanicReport` dengan `confidence_level` opsional via `/admin/schema-version`. `/admin/outage` untuk trigger outage. Auth via `Authorization: Bearer`. | `docker-compose.yml:67-87`; `ARCHITECTURE.md:12` |
| `canonical-store` | — (internal) | `store_net` (internal: true) | PostgreSQL 16+. Satu-satunya pemilik `canonical_events`, `outbox`, `volcano_reference`. | `docker-compose.yml:91-106`; `ARCHITECTURE.md:14` |
| `aggregator` | 8090 (internal saja) | `agency_net`, `store_net`, `internal_api_net`, `bus_net` | Poller + Normalizer + Query API + Outbox Relay. Satu-satunya container yang bergabung ke `store_net`. | `docker-compose.yml:143-180`; `ARCHITECTURE.md:13` |
| `nats-jetstream` | — | `bus_net` (internal) | Broker NATS 2.10 dengan JetStream. Stream `BNPB_EVENTS`. | `docker-compose.yml:127-139`; `ARCHITECTURE.md:15` |
| `notification-consumer` | — | `bus_net` | Konsumen durable; idempotent via bbolt; POST ke `stub-sink`. | `docker-compose.yml:261-283`; `ARCHITECTURE.md:16` |
| `audit-consumer` | — | `bus_net` | Konsumen durable; idempotent via bbolt; arsip event ke bbolt lokal. | `docker-compose.yml:285-303`; `ARCHITECTURE.md:17` |
| `stub-sink` | 8085 (internal) | `bus_net` | Stub HTTP `POST /notify` + `GET /received` untuk verifikasi demo. | `docker-compose.yml:246-259`; `ARCHITECTURE.md:18` |
| `token-service` | 8083 | `edge_net`, `token_net` | Emittent JWT ES256 + refresh token. JWKS endpoint. | `docker-compose.yml:184-213`; `ARCHITECTURE.md:19` |
| `token-db` | — (internal) | `token_net` (internal) | PostgreSQL. Satu-satunya pemilik tabel refresh-token. | `docker-compose.yml:108-123`; `ARCHITECTURE.md:20` |
| `client-api` | 8080 | `edge_net`, `internal_api_net` | Gateway REST klien. Verifikasi JWT lokal, scope projection, rate limiting. | `docker-compose.yml:217-242`; `ARCHITECTURE.md:21` |

### Aturan batasan yang ditegakkan

- **Store exclusivity:** `canonical-store` hanya bergabung `store_net`
  (internal: true); hanya `aggregator` yang bergabung ke jaringan itu
  (`docker-compose.yml:100-101` vs `173`). `token-db` hanya bergabung
  `token_net`; hanya `token-service` (`docker-compose.yml:118` vs `207-208`).
- **Tidak ada akses DB di luar pemilik:** `client-api` tidak memiliki driver
  DB atau kredensial — ia membaca lewat API internal Aggregator
  (`ARCHITECTURE.md:622`, *"client-api holds no DB driver or credentials"*).
- **Port yang dipublikasikan terbatas:** hanya `client-api` (8080) dan
  `token-service` (8083). Mock (8081/8082) dan semua container internal tidak
  mempublikasikan port kecuali untuk kebutuhan demo (`docker-compose.yml:57-58,
  79-80`; `ARCHITECTURE.md:32`).

---

## Alasan — Kenapa PostgreSQL JSONB

Strategi penyimpanan kanonisal menggabungkan kolom bertipe untuk data
inti (severity, area_name, koordinat, waktu) dengan kolom JSONB dinamis
(`attributes`, `sources`) untuk field yang dapat berevolusi, khususnya
`confidence_level` dari PVMBG.

### Skema JSONB

```sql
-- services/canonical-store/migrations/init.sql:27-42
CREATE TABLE IF NOT EXISTS canonical_events (
    hazard_id      TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    ...
    sources        JSONB NOT NULL DEFAULT '{}',  -- {"bmkg": {...}, "pvmbg": {...}}
    attributes     JSONB NOT NULL DEFAULT '{}   -- merged dynamic fields incl. confidence_level
);
```

- JSONB `attributes` memungkinkan penambahan field baru tanpa DDL
  (`ARCHITECTURE.md:44`: *"new fields (e.g. confidence_level) land in JSONB
  with no DDL"*).
- Indeks GIN `jsonb_path_ops` pada `attributes`
  (`init.sql:47-48`) mempercepat query akses field dinamis.
- `sources` JSONB mempertahankan payload mentah per agensi untuk diaudit
  (`init.sql:40`; `canonical.go:111`).

### Transaksi ACID tunggal untuk event + outbox

Setiap upsertkan memasukkan sekaligus baris `outbox` pada transaksi PostgreSQL
yang sama, menjamin konsistensi antara data kanonisal dan pesan yang akan
dipublikasikan:

```go
// services/aggregator/internal/store/canonical.go:95-163
tx, err := r.db.Begin(ctx)
// ... INSERT INTO canonical_events ON CONFLICT ...  (upsert, JSONB merge)
// ... INSERT INTO outbox ...                         (idempotency key hazard_id:version)
// ... tx.Commit()                                    (atomik: keduanya atau tidak ada)
```

Ini adalah keunggulan kritis: *outbox pattern* hanya dapat garantikan
at-least-once delivery bila upsertkan dan insert outbox berada dalam satu transaksi.
Jika salah satu gagal, seluruh operasi rollback
(`ARCHITECTURE.md:131`: *"The Aggregator commits canonical_events upsert +
outbox insert in a single ACID transaction"*).

### Mengapa bukan MongoDB

- MongoDB tidak mendukung transaksi multi-dokumen/atomik yang setara untuk
  kasus ini — upsertkan `canonical_events` + insert `outbox` membutuhkan
  atomisitas transaksional yang nyata.
- PostgreSQL JSONB sudah menyediakan kemampuan skema fleksibel yang dibutuhkan
  tanpa menambah dependensi berbeda dari `canonical-store` yang sudah ada untuk
  `token-db`.
- Arsitektur mematuhi aturan "satu jenis penyimpanan per tipe data" —
  PostgreSQL sudah dipakai untuk toko token, sehingga tidak perlu
  menambah engine lagi.

### Mengapa bukan Cassandra

- Cassandra membutuhkan replikasi konsisten dan partition-key yang
  dipikirkan matang — berlebihan untuk skema demo yang menempati satu host
  dengan volume data yang kecil (22 rekam historis + generator 10 s/cycle).
- Cassandra tidak mendukung transaksi/lokal atomic writes yang diperlukan
  untuk outbox pattern.
- Asumsi `ARCHITECTURE.md:541` dan `ARCHITECTURE.md:9` membatasi deployment
  pada *single host via docker-compose.yml* — Cassandra justru dirancang untuk
  klaster multi-node. Nilai operasionalnya tidak seimbang dengan kebutuhan
  throughput sistem ini (`ARCHITECTURE.md:43`: *"lowest compose footprint"*
  dan `ARCHITECTURE.md:9`).

---

## Batasan

- Skema JSONB menampung field dinamis; **field inti yang bertipe tetap
  (latitude, longitude, occurred_at) tidak boleh diubah tipe datanya secara
  tak terduga** — hal itu membutuhkan migrasi, bukan sekadar penambahan JSONB.
- Penghapusan field JSONB lama tidak otomatis membersihkan data historis
  yang sudah tersimpan; kebijakan tim menunda penghapusan di belakang `Version`.
- `canonical-store` eksklusif — tidak ada service lain yang dapat menulis
  langsung. Semua penulisan harus melalui Aggregator (aturan *store exclusivity*
  pada `ARCHITECTURE.md:619-624`).
- Penyimpanan consumer lokal (bbolt) bersifat per-consumer dan tidak
  direplikasi — restart consumer tidak menghilangkan data di `canonical-store`.

---

## Bukti

### Container & jaringan

- `docker-compose.yml:10-26` mendefinisikan 6 jaringan ter-segmentasi;
  4 di antaranya `internal: true` (`store_net`, `internal_api_net`, `bus_net`,
  `token_net`).
- `canonical-store` dan `token-db` hanya ada pada jaringan internal
  masing-masing: `docker-compose.yml:100-101` (`networks: [store_net]`) dan
  `docker-compose.yml:117-118` (`networks: [token_net]`).
- `aggregator` adalah satu-satunya service yang bergabung ke `store_net`
  sekaligus `internal_api_net` + `bus_net` + `agency_net`
  (`docker-compose.yml:171-175`).

### Penyimpanan JSONB + transaksi tunggal

- Skema tabel: `services/canonical-store/migrations/init.sql:27-42`
  (`canonical_events`) dan `init.sql:56-64` (`outbox` dengan
  `event_key TEXT UNIQUE`).
- Indeks GIN dinamis: `init.sql:47-48`.
- Transaksi atomik upsert + outbox:
  `services/aggregator/internal/store/canonical.go:95` (`Begin`),
  `canonical.go:103-119` (upsert `ON CONFLICT ... DO UPDATE` dengan merge
  JSONB `||`), `canonical.go:152-156` (insert outbox dengan `ON CONFLICT
  DO NOTHING`), `canonical.go:161` (`tx.Commit`).
- Merge JSONB aditif diuji:
  `services/aggregator/internal/store/canonical_test.go:160,228-229`
  (setelah re-ingest, `attributes.confidence_level` menjadi `"medium"` dan
  `extra_attr` tetap ada — bukan replace, melainkan merge).

### Isolasi restart (A7)

- `scripts/demo/down.sh:19` — `docker compose down -v --rmi local`:
  menghentikan semua service, menghapus volume bernama
  (`canonical_store_data`, dsb.) dan image lokal sehingga
  `up.sh` berikutnya dimulai dari database bersih.
- `scripts/demo/up.sh:14-15` — `SERVICES=(bmkg-mock pvmbg-mock
  canonical-store aggregator)`; `up.sh:57` menjalankan
  `docker compose up -d --build --wait`. Aggregator bergantung pada
  kesehatan keempat service lain via `depends_on: condition: service_healthy`
  (`docker-compose.yml:162-170`).
- `scripts/demo/README.md:117-126` — urutan *re-run* (`up.sh` → `smoke.sh` →
  `down.sh`) dan pernyataan: *"Re-run `up.sh` after `down.sh` to verify a
  fresh DB comes back to ≥22 events."*
- `scripts/demo/README.md:49-62` — keluaran ekspektasi `up.sh`
  (keempat service *healthy*).
- Smoke verification A6 (8/8) — `scripts/demo/README.md:79,81` —
  menegaskan semua container kembali sehat dan data kanonisal konsisten
  (`canonical_events`, `outbox`, `volcano_reference`;
  `SELECT count(*) FROM volcano_reference -> 5` pada `smoke.sh:235-237`
  dan `README.md:77`).

### Catatan jumlah container

*Catatan:* issue A8 menyebut "6 container" sebagai target ilustratif;
`docker-compose.yml` yang sebenarnya mendeklarasikan **11 service** (lihat
tabel di atas). Tabel ini merefleksikan kebenaran kode dan jaringan ter-segmentasi
yang mengukur batasan pemilikan data.
