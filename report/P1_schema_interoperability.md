# P1 — Interoperabilitas Skema (Tolerant Reader)

Dokumen ini menjelaskan rancangan **tolerant reader** yang memungkinkan Aggregator
menakdir perubahan skema dari layanan agensi (BMKG, PVMBG) tanpa perlu mendistribusi
perubahan ke seluruh layanan sekaligus. Fokus utama: bagaimana `confidence_level`
dari PVMBG mendarat di `attributes` dan bagaimana `correlation_id` ditelusur
melalui seluruh siklus ingerensi.

---

## Rancangan

Pola toleran-reader diimplementasikan melalui tiga lapisan:

1. **`contracts/event/envelope.go` (Envelope).** Setiap peristahaan baru atau
   diperbarui diterbitkan ke NATS JetStream dibungkus dalam `Envelope` yang
   bersifat *additive* — struktur ini tidak pernah menghapus atau mengganti nama
   field yang ada:

   ```go
   // contracts/event/envelope.go:9-14
   type Envelope struct {
       EventID    string             `json:"event_id"`
       Version    int64              `json:"version"`
       TraceID    string             `json:"trace_id"`    // correlation_id from the ingest cycle
       Data       hazard.HazardEvent `json:"data"`
   }
   ```

   `TraceID` menyiarkan `correlation_id` siklus ingerensi ke konsumen
   downstream (`contracts/event/envelope.go:12`; `services/aggregator/internal/store/canonical.go:139-144`).

2. **`services/aggregator/internal/normalizer/normalizer.go` (Normalizer).**
   Normalizer memetakan field mentah agensi ke dalam `HazardEvent.Attributes`
   (JSONB). Field yang tidak dikenali oleh skema inti secara mekanis dialihkan
   ke `attributes` tanpa transformasi:

   ```go
   // services/aggregator/internal/normalizer/normalizer.go:85-91
   attributes := map[string]interface{}{
       "eruption_count_24h":   report.EruptionCount24h,
       "ash_column_height_m":  report.AshColumnHeightM,
   }
   if report.ConfidenceLevel != nil {
       attributes["confidence_level"] = *report.ConfidenceLevel
   }
   ```

3. **`services/aggregator/internal/store/canonical.go` (Upsert).** Pada level
   penyimpanan, nilai JSONB digabung secara aditif melalui operator PostgreSQL
   `||`, sehingga field baru dari agensi berikutnya selamanya terjepretkan
   tanpa menimpa field yang sudah ada:

   ```sql
   -- services/aggregator/internal/store/canonical.go:111-112
   sources    = canonical_events.sources    || EXCLUDED.sources
   attributes = canonical_events.attributes || EXCLUDED.attributes
   ```

---

## Alasan

**Additive-only dipilih karena upstream (BMKG/PVMBG) berada di luar kontrol
tim, dan skema mereka berevolusi secara independen.**

- Penghapusan atau penggantian nama field memaksa *breaking change* pada setiap
  konsumen yang harus mem-parsingnya. Dengan tolerant reader, yang baru saja
  didukung adalah penambahan key-value (`ARCHITECTURE.md:105-107`).
- `confidence_level` tidak pernah dipaksa ke dalam struktur `VolcanicReport`
  inti secara tak terkontrol; ia adalah `*float64` dengan
  `json:"confidence_level,omitempty"`, sehingga bisa absent secara semantik
  (`services/aggregator/internal/agency/model.go`). Normalizer memutuskan secara implisit:
  pointer `!= nil` — masuk ke `attributes`; pointer `nil` — tidak masuk.
- Karena nilainya selamanya berada di dalam JSONB `attributes`, tidak ada
  migrasi DDL yang dibutuhkan ketika PVMBG menambah field demi field baru
  (`ARCHITECTURE.md:105`; `init.sql:40-41`).
- Penggunaan `Version` (`Envelope.Version`, `canonical_events.version`) dan
  `sources` JSONB (menggabungkan payload mentah per agensi) memastikan data
  mentah asal tetap dapat diauditas bahkan jika struktur `attributes` berubah
  di kemudian hari (`init.sql:40`; `canonical.go:111`).

---

## Batasan

- **Hanya penambahan yang didukung.** Penambahan key baru di JSON upstream
  selalu aman: akan masuk ke `attributes` (atau `sources.<agency>`) tanpa
  perubahan DDL.
- **Penghapusan dan perubahan tipe field inti *tidak* didukung.** `ARCHITECTURE.md:107`
  dan asumsi `ARCHITECTURE.md:544` secara eksplisit mencantumkan
  rename/ removals sebagai *out of scope*. Jika suatu hari agensi menghapus
  `confidence_level`, Aggregator tidak akan lagi menuliskannya — konsumen yang
  bergantung pada kehadirannya wajib menangani ketiadaan field secara defensif.
- **Evolusi berisi nama baru** (`confidence_level` → `trust_score`) tidak
  dilakukan lewat rename — kebijakan tim adalah *bump* `Version` dan/atau
  menambah key baru, bukan mengganti nama yang ada. Penghapusan field lama
  ditunda di belakang `Version` berikutnya.

---

## Bukti

### `confidence_level` mendarat di `attributes`

- Kontrak pointer memungkinkan ketiadaan semantik:
  `services/aggregator/internal/agency/model.go`
  (komentar: *"ConfidenceLevel is a pointer because it may be absent before
  schema-version is toggled"*).
- Normalizer menuliskannya ke `attributes` hanya bila != nil:
  `services/aggregator/internal/normalizer/normalizer.go:89-91`.
- Unit test menegaskan nilai `0.85` berada di `attributes["confidence_level"]`:
  `services/aggregator/internal/normalizer/normalizer_test.go:172,206`
  (`if attrs["confidence_level"].(float64) != 0.85`).
- Saat `ConfidenceLevel == nil`, test memastikan key tidak ada di `attributes`:
  `normalizer_test.go:221,239-240`
  (`if _, ok := rec.Attributes["confidence_level"]; ok { t.Error(...) }`).
- Test integrasi menegaskan merge JSONB menghasilkan nilai terbaru
  (`medium`) dan key baru (`extra_attr`) tetap ada:
  `services/aggregator/internal/store/canonical_test.go:160,228-229`.

### `correlation_id` ditelusur dari ingest ke log hingga envelope

- Setiap siklus *poll* menghasilkan `correlation_id` dan meng‑thread‑nya ke
  record via `rec.CorrelationID = corrID`:
  `services/aggregator/internal/poller/poller.go:123` (BMKG) dan
  `services/aggregator/internal/poller/pvmbg.go:48` (PVMBG).
- `correlation_id` kemudian menjadi `TraceID` pada `Envelope`:
  `contracts/event/envelope.go:12`
  (`json:"trace_id" // correlation_id from the ingest cycle`);
  `services/aggregator/internal/store/canonical.go:142`
  (`TraceID: rec.CorrelationID`).
- Middleware query API memasukkan `correlation_id` ke dalam setiap log
  permintaan: `services/aggregator/internal/queryapi/middleware.go:15`
  (`const correlationIDKey`) dan `middleware.go:76`
  (`Str("correlation_id", corrID)`).
- PVMBG mock juga memotret `correlation_id` di middlewarenya:
  `services/pvmbg-mock/internal/handler/middleware.go:19,106`.

### Verifikasi A6 (smoke) — 8/8 lolos

- `scripts/demo/README.md:79`:
  `Smoke test summary: 8 passed, 0 failed`
  `scripts/demo/README.md:81`: `RESULT: ALL CHECKS PASSED`.
- Hasil tersebut mencakup: `bmkg-mock:8081/health` 200,
  `pvmbg-mock:8082/health` 200, `GET /seismic-events` count=44 (≥22),
  `GET /volcanic-reports` count=44 (≥22) dengan ID `GNG-001..GNG-005`,
  `GET /internal/health` 200 `{"status":"UP"}`,
  `GET /internal/events?limit=100` → `meta.count=44` (≥22) dengan
  `sources.bmkg/pvmbg` `UP` + `last_synced_at`,
  `psql \dt` mencantumkan `canonical_events, outbox, volcano_reference`,
  dan `SELECT count(*) FROM volcano_reference -> 5`.
  (`scripts/demo/README.md:66-82`)
