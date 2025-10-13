from datetime import datetime, timezone
from pathlib import Path
import json
import tempfile

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3

DAG_ID = "stock_eod_to_minio"

default_args = {"owner": "airflow", "retries": 0}

def _fetch_and_upload(**context):
    tickers_param = context["params"].get("tickers", "AAPL,MSFT")
    start_date = context["params"].get("start_date", "2024-01-01")
    end_date = context["params"].get("end_date", "2025-01-01")
    interval_param = context["params"].get("interval", "1d")

    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()]
    if not tickers:
        raise ValueError("No tickers provided")

    bucket = Variable.get("S3_BUCKET", default_var="stock-etl")
    prefix = Variable.get("S3_PREFIX", default_var="raw/eod")

    hook = AwsBaseHook(aws_conn_id="minio_s3", client_type="s3")
    creds = hook.get_credentials()
    extra = hook.get_connection(hook.aws_conn_id).extra_dejson
    endpoint_url = extra.get("endpoint_url")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        endpoint_url=endpoint_url,
        region_name=extra.get("region_name", "us-east-1"),
    )

    load_ts = datetime.now(timezone.utc)
    load_date = load_ts.strftime("%Y-%m-%d")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for t in tickers:
            df = yf.Ticker(t).history(
                start=start_date, 
                end=end_date, 
                interval=interval_param, 
                auto_adjust=True
            )
            if df.empty:
                print(f"[WARN] No data for {t}")
                continue

            df = df.reset_index().rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            })
            df["ticker"] = t
            df["load_ts"] = load_ts.isoformat()

            local_parquet = tmp_path / f"{t}.parquet"
            df.to_parquet(
                local_parquet,
                index=False,
                engine="pyarrow",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True
            )

            key = f"{prefix}/ticker={t}/load_date={load_date}/{t}.parquet"
            print(f"Uploading {local_parquet} to s3://{bucket}/{key}")
            s3.upload_file(str(local_parquet), bucket, key)

    manifest = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval_param,
        "bucket": bucket,
        "prefix": prefix,
        "load_ts": load_ts.isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/_manifests/{load_date}.json",
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "stocks", "minio", "parquet"],
    params={
        "tickers": "MMM,AOS,ABT,ABBV,ACN,ADBE,AMD,AES,AFL,A,APD,ABNB,AKAM,ALB,ARE,ALGN,ALLE,LNT,ALL,GOOGL,GOOG,MO,AMZN,AMCR,AEE,AEP,AXP,AIG,AMT,AWK,AMP,AME,AMGN,APH,ADI,AON,APA,APO,AAPL,AMAT,APP,APTV,ACGL,ADM,ANET,AJG,AIZ,T,ATO,ADSK,ADP,AZO,AVB,AVY,AXON,BKR,BALL,BAC,BAX,BDX,BRK-B,BBY,TECH,BIIB,BLK,BX,XYZ,BK,BA,BKNG,BSX,BMY,AVGO,BR,BRO,BF-B,BLDR,BG,BXP,CHRW,CDNS,CPT,CPB,COF,CAH,KMX,CCL,CARR,CAT,CBOE,CBRE,CDW,COR,CNC,CNP,CF,CRL,SCHW,CHTR,CVX,CMG,CB,CHD,CI,CINF,CTAS,CSCO,C,CFG,CLX,CME,CMS,KO,CTSH,COIN,CL,CMCSA,CAG,COP,ED,STZ,CEG,COO,CPRT,GLW,CPAY,CTVA,CSGP,COST,CTRA,CRWD,CCI,CSX,CMI,CVS,DHR,DRI,DDOG,DVA,DAY,DECK,DE,DELL,DAL,DVN,DXCM,FANG,DLR,DG,DLTR,D,DPZ,DASH,DOV,DOW,DHI,DTE,DUK,DD,EMN,ETN,EBAY,ECL,EIX,EW,EA,ELV,EME,EMR,ETR,EOG,EPAM,EQT,EFX,EQIX,EQR,ERIE,ESS,EL,EG,EVRG,ES,EXC,EXE,EXPE,EXPD,EXR,XOM,FFIV,FDS,FICO,FAST,FRT,FDX,FIS,FITB,FSLR,FE,FI,F,FTNT,FTV,FOXA,FOX,BEN,FCX,GRMN,IT,GE,GEHC,GEV,GEN,GNRC,GD,GIS,GM,GPC,GILD,GPN,GL,GDDY,GS,HAL,HIG,HAS,HCA,DOC,HSIC,HSY,HPE,HLT,HOLX,HD,HON,HRL,HST,HWM,HPQ,HUBB,HUM,HBAN,HII,IBM,IEX,IDXX,ITW,INCY,IR,PODD,INTC,IBKR,ICE,IFF,IP,IPG,INTU,ISRG,IVZ,INVH,IQV,IRM,JBHT,JBL,JKHY,J,JNJ,JCI,JPM,K,KVUE,KDP,KEY,KEYS,KMB,KIM,KMI,KKR,KLAC,KHC,KR,LHX,LH,LRCX,LW,LVS,LDOS,LEN,LII,LLY,LIN,LYV,LKQ,LMT,L,LOW,LULU,LYB,MTB,MPC,MAR,MMC,MLM,MAS,MA,MTCH,MKC,MCD,MCK,MDT,MRK,META,MET,MTD,MGM,MCHP,MU,MSFT,MAA,MRNA,MHK,MOH,TAP,MDLZ,MPWR,MNST,MCO,MS,MOS,MSI,MSCI,NDAQ,NTAP,NFLX,NEM,NWSA,NWS,NEE,NKE,NI,NDSN,NSC,NTRS,NOC,NCLH,NRG,NUE,NVDA,NVR,NXPI,ORLY,OXY,ODFL,OMC,ON,OKE,ORCL,OTIS,PCAR,PKG,PLTR,PANW,PSKY,PH,PAYX,PAYC,PYPL,PNR,PEP,PFE,PCG,PM,PSX,PNW,PNC,POOL,PPG,PPL,PFG,PG,PGR,PLD,PRU,PEG,PTC,PSA,PHM,PWR,QCOM,DGX,RL,RJF,RTX,O,REG,REGN,RF,RSG,RMD,RVTY,HOOD,ROK,ROL,ROP,ROST,RCL,SPGI,CRM,SBAC,SLB,STX,SRE,NOW,SHW,SPG,SWKS,SJM,SW,SNA,SOLV,SO,LUV,SWK,SBUX,STT,STLD,STE,SYK,SMCI,SYF,SNPS,SYY,TMUS,TROW,TTWO,TPR,TRGP,TGT,TEL,TDY,TER,TSLA,TXN,TPL,TXT,TMO,TJX,TKO,TTD,TSCO,TT,TDG,TRV,TRMB,TFC,TYL,TSN,USB,UBER,UDR,ULTA,UNP,UAL,UPS,URI,UNH,UHS,VLO,VTR,VLTO,VRSN,VRSK,VZ,VRTX,VTRS,VICI,V,VST,VMC,WRB,GWW,WAB,WMT,DIS,WBD,WM,WAT,WEC,WFC,WELL,WST,WDC,WY,WSM,WMB,WTW,WDAY,WYNN,XEL,XYL,YUM,ZBRA,ZBH,ZTS",
        "start_date": "2010-10-08",
        "end_date": "2015-10-09",
        "interval": "1d"
    },
) as dag:
    fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=_fetch_and_upload,
        provide_context=True,
    )