# P2 — Model Konkurensi, Circuit Breaker, dan Degradasi yang Halus

Dokumen ini menjelaskan bagaimana Aggregator meng-polling dua agensi (BMKG dan
PVMBG) secara independen, melindungi diri dari kegagalan PVMBG lewat circuit
breaker, dan tetap melayani data pada level terbaru terakhir (degradasi halus)
ketika PVMBG outage tanpa mengembalikan error 500 ke klien.

---

## Model Konkurensi

Polling dilakukan dalam satu pemanggilan `Poll()` yang meluncurkan **satu
goroutine per sumber** dengan timeout masing-masing, menggunakan
`errgroup` dari `golang.org/x/sync`:

```go
// services/aggregator/internal/poller/poller.go:66-91
func (p *Poller) Poll(ctx context.Context, corrID string, since time.Time) {
    ctx, cancel := context.WithTimeout(ctx, p.cfg.PollInterval*2)
    defer cancel()
    eg, ctx := errgroup.WithContext(ctx)

    bmkgCtx, bmkgCancel := context.WithTimeout(ctx, p.cfg.BMKGTimeout)
    defer bmkgCancel()
    eg.Go(func() error {
        p.pollBMKG(bmkgCtx, corrID, since)
        return nil
    })

    pvmbgCtx, pvmbgCancel := context.WithTimeout(ctx, p.cfg.PVMBGTimeout)
    defer pvmbgCancel()
    eg.Go(func() error {
        p.pollPVMBG(pvmbgCtx, corrID, since)
        return nil
    })

    _ = eg.Wait()
}
```

### Timeout per sumber

| Sumber | Timeout | Nilai default | Env var | Kode |
| :--- | :--- | :--- | :--- | :--- |
| BMKG | 1 s | 1000 ms | `BMKG_TIMEOUT_MS` | `config.go:31` |
| PVMBG | 4 s | 4000 ms | `PVMBG_TIMEOUT_MS` | `config.go:34` |

Rationale (1 s vs 4 s): BMKG adalah layanan rendah-latency (50–150 ms jittered);
PVMBG menyuntungkan delay 0–3 s (`pvmbg-mock` env `PVMBG_DELAY_MAX_MS=3000`,
`docker-compose.yml:73-74`), sehingga timeout 4 s memberi 1 s margin
(`ARCHITECTURE.md:135-138`).

### Isolasi kegagalan

- Karena tiap goroutine memiliki `context.WithTimeout`-nya sendiri,
  **kegagalan atau timeout PVMBG tidak pernah memblokir goroutine BMKG**.
  Komentar di `poller.go:70-73` menegaskan ini secara eksplisit:
  *"A slow/failed PVMBG must never block the BMKG goroutine, so BMKG data does
  not go stale while PVMBG is timing out."*
- `bmkgTimeout` dan `pvmbgTimeout` diterapkan pada `http.Client`:
  `poller.go:48` (`Timeout: cfg.BMKGTimeout`) dan `poller.go:49`
  (`Timeout: cfg.PVMBGTimeout`).
- Loop *poll* dijalankan oleh ticker terpisah; setiap siklus menghasilkan
  `correlation_id` baru (`pvmbg.go:75`:
  `corrID := fmt.Sprintf("poll-%d", t.UnixNano())`).

---

## Circuit Breaker (PVMBG)

Circuit breaker diimplementasikan untuk PVMBG pada level *polling loop*
(bukan HTTP client layer), sesuai `ARCHITECTURE.md:141-143`. Mesin statusnya:

```
Closed ──(failureCount >= threshold)──> Open
  ^                                      │
  │                                      │ (probeInterval lalu)
  │                                      ▼
  └── Half-OPEN <──(satu probe berhasil)──
        │
        └──(probe gagal)──> Open (reset timer)
```

### Definisi state dan transisi

- Konstanta state: `StateClosed`, `StateOpen`, `StateHalfOpen`
  (`circuitbreaker/breaker.go:10-14`).
- `Allow()` mengatur logika transisi:
  ```go
  // circuitbreaker/breaker.go:43-61
  switch b.state {
  case StateClosed:
      return true
  case StateOpen:
      if time.Since(b.lastFailureTime) >= b.probeInterval {
          b.state = StateHalfOpen    // <-- Closed -> Open -> HalfOpen
          return true
      }
      return false
  case StateHalfOpen:
      return true
  }
  ```
- `RecordSuccess()` me-reset `failureCount` dan, bila setengah-terbuka,
  kembali ke `StateClosed` (`breaker.go:63-71`).
- `RecordFailure()` men-increment `failureCount`; bila mencapai `threshold`,
  berpindah ke `StateOpen` (`breaker.go:73-88`).

### Parameter (default)

| Parameter | Nilai | Env var | Kode konfigurasi |
| :--- | :--- | :--- | :--- |
| Failure threshold | 3 | `PVMBG_CB_THRESHOLD` | `config.go:35` |
| Probe interval (half-open) | 30 s | `PVMBG_CB_PROBE_INTERVAL` | `config.go:36` |

### Jalur *skip* (open) di `pvmbg.go`

Ketika `Allow()` mengembalikan `false` (state Open), pencarian PVMBG dilewati
dan data terakhir tetap dilayani:

```go
// services/aggregator/internal/poller/pvmbg.go:14-25
if !p.breaker.Allow() {
    p.mu.Lock()
    stale := p.health.PVMBG.LastSynced
    p.health.PVMBG.Status = "OUTAGE"
    if p.health.PVMBG.StaleSince == nil {
        t := stale
        p.health.PVMBG.StaleSince = &t
    }
    p.mu.Unlock()
    p.logger.Warn().Msg("PVMBG circuit breaker open, skipping poll")
    return
}
```

Berhenti tercatat:
- `p.breaker.RecordFailure()` dipanggil saat fetch gagal
  (`pvmbg.go:30`).
- `p.breaker.RecordSuccess()` dipanggil saat fetch berhasil
  (`pvmbg.go:38`).

---

## Degradasi yang Halus (Stale Data)

Pendekatan degradasi halusnya adalah **melayani data terakhir dari Canonical
Store**, bukan mengembalikan error 500.

### Sumber data stale

- `Health()` mengembalikan `SourceStatus` yang memegang tiga bidang:
  `Status`, `LastSynced`, dan `StaleSince *time.Time`
  (`poller.go:20-24`).
- Pada jalur skip di atas, status di-set `OUTAGE` dan `StaleSince` di-isi
  dari `LastSynced` terakhir (`pvmbg.go:17-21`).
- Karena BMKG tetap terhubung dengan timeout 1 s terpisah, data BMKG tidak
  pernah ikut *stale* — hanya PVMBG yang terpengaruh.

### Metadata `stale_since` pada respons `/internal/events`

Handler query API menyertakan metadata keanggotaan sumber pada `meta.sources`:

```go
// services/aggregator/internal/queryapi/handler.go:106-123
if h.sourceHealthFunc != nil {
    bmkgStatus, pvmbgStatus, pvmbgStaleSince := h.sourceHealthFunc()
    // ...
    pvmbgMeta := map[string]interface{}{"status": pvmbgStatus}
    if pvmbgStaleSince != nil {
        pvmbgMeta["stale_since"] = pvmbgStaleSince.Format(time.RFC3339)
    } else {
        pvmbgMeta["last_synced_at"] = time.Now().UTC().Format(time.RFC3339)
    }
    sources["pvmbg"] = pvmbgMeta
    resp.Meta["sources"] = sources
}
```

Dengan demikian klien memperoleh sinyal eksplisit bahwa data PVMBG sudah tidak
baru, sambil tetap menerima data BMKG yang segar. Bentuk respons yang
dokumenjanlakan (`ARCHITECTURE.md:255-259`):

```json
"pvmbg": { "status": "OUTAGE", "stale_since": "2026-09-21T22:35:00Z" }
```

### Verifikasi unit test

- `handler_test.go:432-441`: memastikan `stale_since` ada pada
  `meta.sources.pvmbg` saat `SourceHealthFunc` melaporkan outage, dan
  `last_synced_at` tidak ada pada kasus tersebut.
- `handler_test.go:496-497`: memastikan `stale_since` tidak ada ketika
  sumber *not* stale.

### Bukti operasional (README demo)

- `README.md:65-75` menggambarkan skenario outage: toggle outage, query
  kejadian — harus kembali data *stale* dengan `stale_since`, **bukan 500**.
  (`README.md:70` komentar: *"should return stale data with stale_since,
  not 500"*)
