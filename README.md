# sgt-dashboard

Dash-based monitoring dashboard for market data published into SGT shared memory.

## Package contents

- `sgt_dashboard/shm_direct_price_reader.py`: direct SysV SHM reader that streams decoded events.
- `sgt_dashboard/shm_dash_app.py`: Dash UI for live bid/ask/trade/notional monitoring.

## Install

```bash
cd ~/repos/sgt-dashboard
python3 -m pip install -e .
```

## Run dashboard

```bash
sgt-shm-dash \
  --pathname /node_data/shm/datashm \
  --refdata /home/jdlee/workspace/refdata/crypto_symbology/refdata.latest.json \
  --host 127.0.0.1 \
  --port 8060 \
  --title "Price Monitor"
```

## Run reader directly

```bash
sgt-shm-reader --pathname /node_data/shm/datashm --refdata /path/to/refdata.json
```

## Notes

- The dashboard polls the reader stream on a Dash interval (`500ms` by default).
- Volatility is computed from the 10-minute mid-price series and shown in percent.
- Estimated 24-hour volume is extrapolated from the latest 10-minute notional series.


## Screenshots

### BTC BYBIT 1m

<p>
  <img src="pictures/screenshot_btc_bybit_1m.png" alt="BTC BYBIT 1m" width="700" />
</p>

### SOL BYBIT 10m

<p>
  <img src="pictures/screenshot_sol_bybit_10m.png" alt="SOL BYBIT 10m" width="700" />
</p>

## Testing

```bash
cd ~/repos/sgt-dashboard
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```
