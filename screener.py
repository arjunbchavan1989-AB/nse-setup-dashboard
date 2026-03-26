import yfinance as yf
import pandas as pd
import numpy as np
import json, time, warnings
warnings.filterwarnings('ignore')

# ── Complete Nifty MidSmallcap 400 Symbol List (NSE) ─────────────────────────
SYMBOLS = [
    "360ONE","3MINDIA","ACC","ACMESOLAR","AIAENG","APLAPOLLO","AUBANK","AWL",
    "AADHARHFC","AARTIIND","AAVAS","ABBOTINDIA","ACE","ATGL","ABCAPITAL","ABFRL",
    "ABLBL","ABREL","ABSLAMC","AEGISLOG","AEGISVOPAK","AFCONS","AFFLE","AJANTPHARM",
    "AKUMS","AKZOINDIA","APLLTD","ALKEM","ALKYLAMINE","ALOKINDS","ARE&M","AMBER",
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
    "FIVESTAR","FORCEMOT","FORTIS","GVT&D","GMRAIRPORT","GRSE","GICRE","GILLETTE",
    "GLAND","GLAXO","GLENMARK","MEDANTA","GODIGIT","GPIL","GODFRYPHLP","GODREJAGRO",
    "GODREJIND","GODREJPROP","GRANULES","GRAPHITE","GRAVITA","GESHIP","FLUOROCHEM",
    "GUJGASLTD","GMDCLTD","GSPL","HEG","HBLENGINE","HDFCAMC","HFCL","HAPPSTMNDS",
    "HEROMOTOCO","HEXT","HSCL","HINDCOPPER","HINDPETRO","POWERINDIA","HOMEFIRST",
    "HONASA","HONAUT","HUDCO","ICICIPRULI","IDBI","IDFCFIRSTB","IFCI","IIFL",
    "INOXINDIA","IRB","IRCON","ITCHOTELS","ITI","INDGN","INDIACEM","INDIAMART",
    "INDIANB","IEX","IOB","IRCTC","IREDA","IGL","INDUSTOWER","INDUSINDBK","INOXWIND",
    "INTELLECT","IGIL","IKS","IPCALAB","JBCHEPHARM","JKCEMENT","JBMA","JKTYRE",
    "JMFINANCIL","JSWINFRA","JPPOWER","J&KBANK","JINDALSAW","JSL","JUBLFOOD",
    "JUBLINGREA","JUBLPHARMA","JWL","JYOTHYLAB","JYOTICNC","KPRMILL","KEI",
    "KPITTECH","KSB","KAJARIACER","KPIL","KALYANKJIL","KARURVYSYA","KAYNES","KEC",
    "KFINTECH","KIRLOSBROS","KIRLOSENG","KIMS","LTF","LTTS","LICHSGFIN","LTFOODS",
    "LATENTVIEW","LAURUSLABS","THELEELA","LEMONTREE","LINDEINDIA","LLOYDSME","LUPIN",
    "MMTC","MRF","MGL","MAHSCOOTER","MAHSEAMLES","M&MFIN","MANAPPURAM","MRPL",
    "MANKIND","MARICO","MFSL","METROPOLIS","MINDACORP","MSUMI","MOTILALOFS","MPHASIS",
    "MCX","MUTHOOTFIN","NATCOPHARM","NBCC","NCC","NHPC","NLCINDIA","NMDC","NSLNISP",
    "NTPCGREEN","NH","NATIONALUM","NAVA","NAVINFLUOR","NETWEB","NEULANDLAB","NEWGEN",
    "NAM-INDIA","NIVABUPA","NUVAMA","NUVOCO","OBEROIRLTY","OIL","OLAELEC","OLECTRA",
    "PAYTM","OFSS","POLICYBZR","PCBL","PGEL","PIIND","PNBHOUSING","PTCIL","PVRINOX",
    "PAGEIND","PATANJALI","PERSISTENT","PETRONET","PFIZER","PHOENIXLTD","PPLPHARMA",
    "POLYMED","POLYCAB","POONAWALLA","PRAJIND","PREMIERENE","PRESTIGE","PGHH",
    "RRKABEL","RBLBANK","RHIM","RITES","RADICO","RVNL","RAILTEL","RAINBOW","RKFORGE",
    "RCF","REDINGTON","RPOWER","SBFC","SBICARD","SJVN","SRF","SAGILITY","SAILIFE",
    "SAMMAANCAP","SAPPHIRE","SARDAEN","SAREGAMA","SCHAEFFLER","SCHNEIDER","SCI",
    "SHYAMMETL","SIGNATURE","SOBHA","SONACOMS","SONATSOFTW","STARHEALTH","SAIL",
    "SUMICHEM","SUNTV","SUNDARMFIN","SUNDRMFAST","SUPREMEIND","SUZLON","SWIGGY",
    "SYNGENE","SYRMA","TBOTEK","TATACHEM","TATACOMM","TATAELXSI","TATAINVEST",
    "TATATECH","TECHNOE","TEJASNET","NIACL","RAMCOCEM","THERMAX","TIMKEN","TITAGARH",
    "TORNTPOWER","TARIL","TRIDENT","TRIVENI","TRITURBINE","TIINDIA","UCOBANK",
    "UNOMINDA","UPL","UTIAMC","UNIONBANK","UBL","USHAMART","VGUARD","DBREALTY",
    "VTL","MANYAVAR","VENTIVE","VIJAYA","VMM","IDEA","VOLTAS","WAAREEENER","WELCORP",
    "WELSPUNLIV","WHIRLPOOL","WOCKPHARMA","YESBANK","ZFCVINDIA","ZEEL","ZENTEC",
    "ZENSARTECH","ECLERX"
]

# ── Setup Detection Functions ─────────────────────────────────────────────────

def detect_bull_flag(df):
    if len(df) < 20: return False, 0
    c = df['Close'].values
    pole = (c[-11] - c[-21]) / c[-21] * 100 if c[-21] > 0 else 0
    flag_range = (max(c[-6:-1]) - min(c[-6:-1])) / min(c[-6:-1]) * 100 if min(c[-6:-1]) > 0 else 99
    vol = df['Volume'].values
    avg_vol = vol[-21:-6].mean()
    flag_vol = vol[-6:-1].mean()
    vol_contraction = flag_vol < avg_vol * 0.8
    score = round(min(100, max(0, (pole * 3) - (flag_range * 5) + (20 if vol_contraction else 0))), 1)
    return pole > 8 and flag_range < 6 and vol_contraction, score

def detect_high_tight_flag(df):
    if len(df) < 60: return False, 0
    c = df['Close'].values
    pole_gain = (c[-21] - c[-61]) / c[-61] * 100 if c[-61] > 0 else 0
    consolidation = (max(c[-21:]) - min(c[-21:])) / max(c[-21:]) * 100 if max(c[-21:]) > 0 else 99
    score = round(min(100, max(0, (pole_gain / 2) - (consolidation * 1.5))), 1)
    return pole_gain > 90 and consolidation < 25, score

def detect_power_earnings_gap(df):
    if len(df) < 30: return False, 0
    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    avg_vol = v[-30:-1].mean()
    best_score, triggered = 0, False
    for i in range(max(1, len(c)-40), len(c)-1):
        gap_pct = (o[i] - c[i-1]) / c[i-1] * 100 if c[i-1] > 0 else 0
        vol_ratio = v[i] / avg_vol if avg_vol > 0 else 0
        if gap_pct > 5 and vol_ratio > 2:
            held = all(c[j] > c[i-1] for j in range(i, min(i+5, len(c))))
            s = round(min(100, gap_pct * 4 + vol_ratio * 5), 1)
            if s > best_score:
                best_score, triggered = s, held
    return triggered, best_score

def detect_flat_base_breakout(df):
    if len(df) < 35: return False, 0
    c = df['Close'].values
    v = df['Volume'].values
    base = c[-26:-1]
    base_range = (max(base) - min(base)) / min(base) * 100 if min(base) > 0 else 99
    breakout = c[-1] > max(base)
    vol_surge = v[-1] > v[-26:-1].mean() * 1.3
    prior_trend = (c[-26] - c[-51]) / c[-51] * 100 if len(c) > 51 and c[-51] > 0 else 0
    score = round(min(100, max(0, 60 - base_range * 2 + (20 if breakout else 0) + (15 if vol_surge else 0) + (prior_trend * 0.3))), 1)
    return base_range < 15 and breakout and vol_surge, score

# ── Batch Downloader (processes in chunks of 100) ────────────────────────────

def download_in_batches(symbols, batch_size=100):
    """Downloads data in batches to avoid timeouts with 400 stocks."""
    all_data = {}
    total_batches = (len(symbols) + batch_size - 1) // batch_size

    for i in range(0, len(symbols), batch_size):
        batch_num = i // batch_size + 1
        batch = symbols[i:i + batch_size]
        tickers = [s + ".NS" for s in batch]
        print(f"  Batch {batch_num}/{total_batches}: downloading {len(tickers)} stocks...", end=" ")

        try:
            raw = yf.download(
                tickers,
                period="4mo",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True
            )

            for sym, ticker in zip(batch, tickers):
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        df = raw[ticker].copy().dropna(subset=['Close'])
                    else:
                        df = raw.copy().dropna(subset=['Close'])
                    if len(df) >= 40:
                        all_data[sym] = df
                except:
                    continue

            print(f"OK — {sum(1 for s in batch if s in all_data)} valid")

        except Exception as e:
            print(f"FAILED: {e}")

        # Small delay between batches to be polite to Yahoo Finance
        if batch_num < total_batches:
            time.sleep(2)

    return all_data

# ── Main Scan ─────────────────────────────────────────────────────────────────

def run_scan():
    print(f"Starting scan for {len(SYMBOLS)} Nifty MidSmallcap 400 stocks...")
    print("Downloading in batches of 100 (4 batches total)...\n")

    stocks_data = download_in_batches(SYMBOLS, batch_size=100)
    print(f"\nTotal valid stocks: {len(stocks_data)}")
    print("Running setup detection...\n")

    results = []
    for sym, df in stocks_data.items():
        try:
            close  = df['Close'].values
            volume = df['Volume'].values

            chg_1d = round((close[-1] - close[-2]) / close[-2] * 100, 2) if close[-2] > 0 else 0
            chg_1m = round((close[-1] - close[-22]) / close[-22] * 100, 2) if len(close) > 22 and close[-22] > 0 else 0
            chg_3m = round((close[-1] - close[-63]) / close[-63] * 100, 2) if len(close) > 63 and close[-63] > 0 else 0
            avg_vol_20 = int(volume[-20:].mean())
            rel_vol = round(volume[-1] / avg_vol_20, 2) if avg_vol_20 > 0 else 0

            bf_trig,  bf_score  = detect_bull_flag(df)
            htf_trig, htf_score = detect_high_tight_flag(df)
            peg_trig, peg_score = detect_power_earnings_gap(df)
            fbb_trig, fbb_score = detect_flat_base_breakout(df)

            setup_scores = {
                "Bull Flag":          bf_score  if bf_trig  else 0,
                "High Tight Flag":    htf_score if htf_trig else 0,
                "Power Earnings Gap": peg_score if peg_trig else 0,
                "Flat Base Breakout": fbb_score if fbb_trig else 0,
            }
            active_setups  = {k: v for k, v in setup_scores.items() if v > 0}
            primary_setup  = max(active_setups, key=active_setups.get) if active_setups else "No Setup"
            primary_score  = active_setups.get(primary_setup, 0)

            sparkline = [round(float(x), 2) for x in close[-60:]]

            results.append({
                "symbol":        sym,
                "price":         round(float(close[-1]), 2),
                "chg_1d":        chg_1d,
                "chg_1m":        chg_1m,
                "chg_3m":        chg_3m,
                "rel_vol":       rel_vol,
                "avg_vol":       avg_vol_20,
                "primary_setup": primary_setup,
                "primary_score": primary_score,
                "setups": {
                    "Bull Flag":          {"triggered": bf_trig,  "score": bf_score},
                    "High Tight Flag":    {"triggered": htf_trig, "score": htf_score},
                    "Power Earnings Gap": {"triggered": peg_trig, "score": peg_score},
                    "Flat Base Breakout": {"triggered": fbb_trig, "score": fbb_score},
                },
                "sparkline": sparkline
            })
        except Exception as e:
            continue

    results.sort(key=lambda x: x['primary_score'], reverse=True)

    # Summary
    print(f"{'='*55}")
    print(f"  SCAN COMPLETE — {len(results)} stocks processed")
    print(f"{'='*55}")
    print(f"  🚩 Bull Flag:          {sum(1 for r in results if r['setups']['Bull Flag']['triggered'])}")
    print(f"  🔥 High Tight Flag:    {sum(1 for r in results if r['setups']['High Tight Flag']['triggered'])}")
    print(f"  ⚡ Power Earnings Gap: {sum(1 for r in results if r['setups']['Power Earnings Gap']['triggered'])}")
    print(f"  📐 Flat Base Breakout: {sum(1 for r in results if r['setups']['Flat Base Breakout']['triggered'])}")
    print(f"{'='*55}\n")

    return results

if __name__ == "__main__":
    data = run_scan()
    with open("stocks_data.json", "w") as f:
        json.dump(data, f)
    print("✅ Saved to stocks_data.json — open dashboard.html in browser.")