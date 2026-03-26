import yfinance as yf
import pandas as pd
import numpy as np
import json
import time
import warnings
warnings.filterwarnings("ignore")

# ── Nifty MidSmallcap 400 — Complete Symbol List (NSE, March 2026) ──────────
SYMBOLS = [
    "360ONE","3MINDIA","ACC","ACMESOLAR","AIAENG","APLAPOLLO","AUBANK","AWL",
    "AADHARHFC","AARTIIND","AAVAS","ABBOTINDIA","ACE","ATGL","ABCAPITAL","ABFRL",
    "ABLBL","ABREL","ABSLAMC","AEGISLOG","AEGISVOPAK","AFCONS","AFFLE","AJANTPHARM",
    "AKUMS","AKZOINDIA","APLLTD","ALKEM","ALKYLAMINE","ALOKINDS","AMBER",
    "ANANDRATHI","ANANTRAJ","ANGELONE","APARINDS","APOLLOTYRE","APTUS","ASAHIINDIA",
    "ASHOKLEY","ASTERDM","ASTRAZEN","ASTRAL","ATHERENERG","ATUL","AUROPHARMA","AIIL",
    "BASF","BEML","BLS","BSE","BALKRISIND","BALRAMCHIN","BANDHANBNK","BANKINDIA",
    "MAHABANK","BATAINDIA","BAYERCROP","BERGEPAINT","BDL","BHARATFORG","BHEL",
    "BHARTIHEXA","BIKAJI","BIOCON","BSOFT","BLUEDART","BLUEJET","BLUESTARCO",
    "BBTC","FIRSTCRY","BRIGADE","MAPMYINDIA","CCL","CESC","CRISIL","CAMPUS",
    "CANFINHOME","CAPLIPOINT","CGCL","CARBORUNIV","CASTROLIND","CEATLTD","CENTRALBK",
    "CDSL","CENTURYPLY","CERA","CHALET","CHAMBLFERT","CHENNPETRO","CHOICEIN",
    "CHOLAHLDNG","CUB","CLEAN","COCHINSHIP","COFORGE","COLPAL","CAMS","CONCORDBIO",
    "CONCOR","COROMANDEL","CRAFTSMAN","CREDITACC","CROMPTON","CUMMINSIND","CYIENT",
    "DCMSHRIRAM","DOMS","DABUR","DALBHARAT","DATAPATTNS","DEEPAKFERT","DEEPAKNTR",
    "DELHIVERY","DEVYANI","DIXON","AGARWALEYE","LALPATHLAB","EIDPARRY","EIHOTEL",
    "ELECON","ELGIEQUIP","EMAMILTD","EMCURE","ENDURANCE","ENGINERSIN","ERIS",
    "ESCORTS","EXIDEIND","NYKAA","FEDERALBNK","FACT","FINCABLES","FINPIPE","FSL",
    "FIVESTAR","FORCEMOT","FORTIS","GMRAIRPORT","GRSE","GICRE","GILLETTE",
    "GLAND","GLAXO","GLENMARK","MEDANTA","GODIGIT","GPIL","GODFRYPHLP","GODREJAGRO",
    "GODREJIND","GODREJPROP","GRANULES","GRAPHITE","GRAVITA","GESHIP","FLUOROCHEM",
    "GUJGASLTD","GMDCLTD","GSPL","HEG","HBLENGINE","HDFCAMC","HFCL","HAPPSTMNDS",
    "HEROMOTOCO","HSCL","HINDCOPPER","HINDPETRO","POWERINDIA","HOMEFIRST",
    "HONASA","HONAUT","HUDCO","ICICIPRULI","IDBI","IDFCFIRSTB","IFCI","IIFL",
    "INOXINDIA","IRB","IRCON","ITCHOTELS","ITI","INDGN","INDIACEM","INDIAMART",
    "INDIANB","IEX","IOB","IRCTC","IREDA","IGL","INDUSTOWER","INDUSINDBK","INOXWIND",
    "INTELLECT","IPCALAB","JBCHEPHARM","JKCEMENT","JBMA","JKTYRE",
    "JMFINANCIL","JSWINFRA","JPPOWER","JINDALSAW","JSL","JUBLFOOD",
    "JUBLINGREA","JUBLPHARMA","JWL","JYOTHYLAB","JYOTICNC","KPRMILL","KEI",
    "KSB","KAJARIACER","KPIL","KALYANKJIL","KARURVYSYA","KAYNES","KEC",
    "KFINTECH","KIRLOSBROS","KIRLOSENG","KIMS","LTF","LTTS","LICHSGFIN","LTFOODS",
    "LATENTVIEW","LAURUSLABS","THELEELA","LEMONTREE","LINDEINDIA","LLOYDSME","LUPIN",
    "MMTC","MRF","MGL","MAHSCOOTER","MAHSEAMLES","MANAPPURAM","MRPL",
    "MANKIND","MARICO","MFSL","METROPOLIS","MINDACORP","MSUMI","MOTILALOFS","MPHASIS",
    "MCX","MUTHOOTFIN","NATCOPHARM","NBCC","NCC","NHPC","NLCINDIA","NMDC","NSLNISP",
    "NTPCGREEN","NH","NATIONALUM","NAVA","NAVINFLUOR","NETWEB","NEULANDLAB","NEWGEN",
    "NIVABUPA","NUVAMA","NUVOCO","OBEROIRLTY","OIL","OLAELEC","OLECTRA",
    "PAYTM","OFSS","POLICYBZR","PCBL","PGEL","PIIND","PNBHOUSING","PTCIL","PVRINOX",
    "PAGEIND","PATANJALI","PERSISTENT","PETRONET","PFIZER","PHOENIXLTD","PPLPHARMA",
    "POLYMED","POLYCAB","POONAWALLA","PRAJIND","PREMIERENE","PRESTIGE","PGHH",
    "RRKABEL","RBLBANK","RHIM","RITES","RADICO","RVNL","RAILTEL","RAINBOW","RKFORGE",
    "RCF","REDINGTON","RPOWER","SBFC","SBICARD","SJVN","SRF","SAGILITY",
    "SAPPHIRE","SARDAEN","SAREGAMA","SCHAEFFLER","SCHNEIDER","SCI",
    "SHYAMMETL","SOBHA","SONACOMS","SONATSOFTW","STARHEALTH","SAIL",
    "SUMICHEM","SUNTV","SUNDARMFIN","SUNDRMFAST","SUPREMEIND","SUZLON","SWIGGY",
    "SYNGENE","SYRMA","TATACHEM","TATACOMM","TATAELXSI","TATAINVEST",
    "TATATECH","TECHNOE","TEJASNET","NIACL","RAMCOCEM","THERMAX","TIMKEN","TITAGARH",
    "TORNTPOWER","TRIDENT","TRIVENI","TRITURBINE","TIINDIA","UCOBANK",
    "UNOMINDA","UPL","UTIAMC","UNIONBANK","UBL","USHAMART","VGUARD","DBREALTY",
    "VTL","MANYAVAR","VENTIVE","IDEA","VOLTAS","WAAREEENER","WELCORP",
    "WELSPUNLIV","WHIRLPOOL","WOCKPHARMA","YESBANK","ZFCVINDIA","ZEEL",
    "ZENSARTECH","ECLERX","NAM-INDIA","M&MFIN.NS","J&KBANK.NS","AREINDIA","GVT&D.NS","KPITTECH"
]

# Remove duplicates while preserving order
seen = set()
SYMBOLS = [s for s in SYMBOLS if not (s in seen or seen.add(s))]
print(f"Total symbols to scan: {len(SYMBOLS)}")


# ── Setup Detection Functions ─────────────────────────────────────────────────

def detect_bull_flag(df):
    """Pole: >8% gain in 10 days. Flag: <6% consolidation range + volume contraction."""
    if len(df) < 22:
        return False, 0
    c = df["close"].values
    v = df["volume"].values
    pole         = (c[-11] - c[-21]) / c[-21] * 100 if c[-21] > 0 else 0
    f_high       = max(c[-6:-1])
    f_low        = min(c[-6:-1])
    flag_range   = (f_high - f_low) / f_low * 100 if f_low > 0 else 99
    avg_vol      = v[-21:-6].mean()
    flag_vol     = v[-6:-1].mean()
    vol_contract = flag_vol < avg_vol * 0.8
    score = round(min(100, max(0,
        pole * 3 - flag_range * 5 + (20 if vol_contract else 0)
    )), 1)
    triggered = pole > 8 and flag_range < 6 and vol_contract
    return triggered, score


def detect_high_tight_flag(df):
    """Pole: >90% gain in 8 weeks (40 days). Flag: <25% drawdown over last 4 weeks."""
    if len(df) < 62:
        return False, 0
    c             = df["close"].values
    pole_gain     = (c[-21] - c[-61]) / c[-61] * 100 if c[-61] > 0 else 0
    flag_high     = max(c[-21:])
    flag_low      = min(c[-21:])
    consolidation = (flag_high - flag_low) / flag_high * 100 if flag_high > 0 else 99
    score         = round(min(100, max(0, (pole_gain / 2) - (consolidation * 1.5))), 1)
    triggered     = pole_gain > 90 and consolidation < 25
    return triggered, score


def detect_power_earnings_gap(df):
    """Gap up >5% on volume >2x average, price holds above gap level for 5 days."""
    if len(df) < 32:
        return False, 0
    c          = df["close"].values
    o          = df["open"].values
    v          = df["volume"].values
    avg_vol    = v[-30:-1].mean()
    best_score = 0
    triggered  = False
    for i in range(max(1, len(c) - 45), len(c) - 1):
        prev_close = c[i - 1]
        if prev_close <= 0:
            continue
        gap_pct   = (o[i] - prev_close) / prev_close * 100
        vol_ratio = v[i] / avg_vol if avg_vol > 0 else 0
        if gap_pct > 5 and vol_ratio > 2:
            end_idx = min(i + 6, len(c))
            held    = all(c[j] > prev_close for j in range(i, end_idx))
            s       = round(min(100, gap_pct * 4 + vol_ratio * 5), 1)
            if s > best_score:
                best_score = s
                triggered  = held
    return triggered, best_score


def detect_flat_base_breakout(df):
    """Base: <15% range for 5 weeks after uptrend. Breakout: close > base high on volume."""
    if len(df) < 52:
        return False, 0
    c          = df["close"].values
    v          = df["volume"].values
    base       = c[-26:-1]
    base_high  = max(base)
    base_low   = min(base)
    if base_low <= 0 or base_high <= 0:
        return False, 0
    base_range    = (base_high - base_low) / base_low * 100
    breakout      = c[-1] > base_high
    avg_base_vol  = v[-26:-1].mean()
    vol_surge     = v[-1] > avg_base_vol * 1.3 if avg_base_vol > 0 else False
    prior_trend   = (c[-26] - c[-51]) / c[-51] * 100 if c[-51] > 0 else 0
    score = round(min(100, max(0,
        60 - base_range * 2
        + (20 if breakout  else 0)
        + (15 if vol_surge else 0)
        + prior_trend * 0.3
    )), 1)
    triggered = base_range < 15 and breakout and vol_surge
    return triggered, score


# ── Safe DataFrame Extractor ──────────────────────────────────────────────────

def extract_df(raw, ticker):
    """Safely extract a single stock DataFrame from batch download result."""
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            level0 = raw.columns.get_level_values(0).tolist()
            level1 = raw.columns.get_level_values(1).tolist()
            if ticker in level0:
                df = raw[ticker].copy()
            elif ticker in level1:
                df = raw.xs(ticker, axis=1, level=1).copy()
            else:
                return None
        else:
            df = raw.copy()

        df = df.dropna(subset=["Close"])

        # Normalise all column names to lowercase
        df.columns = [str(c).strip().lower() for c in df.columns]

        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(set(df.columns)):
            return None
        if len(df) < 40:
            return None
        return df

    except Exception:
        return None


# ── Batch Downloader ──────────────────────────────────────────────────────────

def download_in_batches(symbols, batch_size=80):
    """Download in batches of 80 to avoid timeouts and rate limits."""
    all_data    = {}
    total       = len(symbols)
    num_batches = (total + batch_size - 1) // batch_size

    for batch_num, start in enumerate(range(0, total, batch_size), 1):
        batch   = symbols[start : start + batch_size]
        tickers = [s + ".NS" for s in batch]


        print(f"  Batch {batch_num}/{num_batches}: {len(tickers)} stocks ...", end=" ")

        try:
            raw = yf.download(
                tickers,
                period      = "4mo",
                interval    = "1d",
                group_by    = "ticker",
                auto_adjust = True,
                progress    = False,
                threads     = True,
            )

            if raw is None or raw.empty:
                print("EMPTY — skipping")
                continue

            ok = 0
            for sym, ticker in zip(batch, tickers):
                df = extract_df(raw, ticker)
                if df is not None:
                    all_data[sym] = df
                    ok += 1

            print(f"OK ({ok}/{len(batch)} valid)")

        except Exception as e:
            print(f"FAILED: {e}")

        if batch_num < num_batches:
            time.sleep(3)

    return all_data


# ── Main Scan ─────────────────────────────────────────────────────────────────

def run_scan():
    print("=" * 60)
    print(f"  NSE Setup Scanner — Nifty MidSmallcap 400")
    print(f"  Scanning {len(SYMBOLS)} stocks")
    print("=" * 60)

    stocks_data = download_in_batches(SYMBOLS, batch_size=80)

    print(f"\nDownload complete — {len(stocks_data)} valid stocks")
    print("Running setup detection...\n")

    results = []

    for sym, df in stocks_data.items():
        try:
            c = df["close"].values
            v = df["volume"].values

            chg_1d = round((c[-1] - c[-2])  / c[-2]  * 100, 2) if len(c) > 1  and c[-2]  > 0 else 0
            chg_1m = round((c[-1] - c[-22]) / c[-22] * 100, 2) if len(c) > 22 and c[-22] > 0 else 0
            chg_3m = round((c[-1] - c[-63]) / c[-63] * 100, 2) if len(c) > 63 and c[-63] > 0 else 0

            avg_vol_20 = int(v[-20:].mean()) if len(v) >= 20 else int(v.mean())
            rel_vol    = round(v[-1] / avg_vol_20, 2) if avg_vol_20 > 0 else 0

            bf_trig,  bf_score  = detect_bull_flag(df)
            htf_trig, htf_score = detect_high_tight_flag(df)
            peg_trig, peg_score = detect_power_earnings_gap(df)
            fbb_trig, fbb_score = detect_flat_base_breakout(df)

            setup_scores = {
                "Bull Flag"         : bf_score  if bf_trig  else 0,
                "High Tight Flag"   : htf_score if htf_trig else 0,
                "Power Earnings Gap": peg_score if peg_trig else 0,
                "Flat Base Breakout": fbb_score if fbb_trig else 0,
            }
            active        = {k: val for k, val in setup_scores.items() if val > 0}
            primary       = max(active, key=active.get) if active else "No Setup"
            primary_score = active.get(primary, 0)

            sparkline = [round(float(x), 2) for x in c[-60:]]

            results.append({
                "symbol"       : sym,
                "price"        : round(float(c[-1]), 2),
                "chg_1d"       : chg_1d,
                "chg_1m"       : chg_1m,
                "chg_3m"       : chg_3m,
                "rel_vol"      : rel_vol,
                "avg_vol"      : avg_vol_20,
                "primary_setup": primary,
                "primary_score": primary_score,
                "setups": {
                    "Bull Flag"         : {"triggered": bool(bf_trig),  "score": bf_score},
                    "High Tight Flag"   : {"triggered": bool(htf_trig), "score": htf_score},
                    "Power Earnings Gap": {"triggered": bool(peg_trig), "score": peg_score},
                    "Flat Base Breakout": {"triggered": bool(fbb_trig), "score": fbb_score},
                },
                "sparkline": sparkline,
            })

        except Exception as e:
            print(f"  Warning — skipped {sym}: {e}")
            continue

    results.sort(key=lambda x: x["primary_score"], reverse=True)

    print("=" * 60)
    print(f"  SCAN COMPLETE — {len(results)} stocks processed")
    print("=" * 60)
    print(f"  🚩 Bull Flag         : {sum(1 for r in results if r['setups']['Bull Flag']['triggered'])}")
    print(f"  🔥 High Tight Flag   : {sum(1 for r in results if r['setups']['High Tight Flag']['triggered'])}")
    print(f"  ⚡ Power Earnings Gap: {sum(1 for r in results if r['setups']['Power Earnings Gap']['triggered'])}")
    print(f"  📐 Flat Base Breakout: {sum(1 for r in results if r['setups']['Flat Base Breakout']['triggered'])}")
    print("=" * 60)

    return results


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        data = run_scan()
        if data:
            with open("stocks_data.json", "w") as f:
                json.dump(data, f)
            print(f"\n✅ Saved {len(data)} stocks to stocks_data.json")
            print("   Open dashboard.html in your browser to view.")
        else:
            print("\n⚠️  No data returned — stocks_data.json not updated.")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise
