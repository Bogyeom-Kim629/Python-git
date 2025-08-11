"""
Author: Bogyeom Kim
Date: 2025-08-01
Purpose: Korea TAM update by yearly
"""

import os
import sys
import time
import json
import requests
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────────────
def fetch_with_retry(url, max_retries=3, backoff_factor=1):
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r
        except requests.RequestException:
            pass
        if attempt < max_retries:
            time.sleep(backoff_factor * 2**(attempt-1))
    return None
# ─────────────────────────────────────────────────────

def fetch_hira(base_dir):
    """2019~2024 HIRA: diagCdNm == '계' (Total Patients)만 추출"""
    in_file = os.path.join(base_dir, "TAM Matching Code_2023.xlsx")
    if not os.path.exists(in_file):
        sys.exit(f"[HIRA] Input not found: {in_file}")

    proc = pd.read_excel(in_file, sheet_name=0, engine="openpyxl")
    # --- dynamically find the right columns ---
    cols = proc.columns.tolist()
    code_col = next((c for c in cols if 'code' in c.lower()), cols[0])
    subj_col = next((c for c in cols if 'subject' in c.lower()), cols[min(1, len(cols)-1)])

    service = "http://apis.data.go.kr/B551182/mdlrtActionInfoService/getMdlrtActionByClassesStats"
    key     = "YxL21B8kZNDhiLwgdLEK0KPM3Rrn6GQTINYu5apbBcEXEimmcedsthnFI5d9YRDLtXHu1fZHfg07oeDxCKJoXA%3D%3D"
    years   = [str(y) for y in range(2019,2025)]
    rows, pageNo, stdType = 10, 1, "1"

    records = []
    for year in years:
        for code, subj in zip(proc[code_col], proc[subj_col]):
            url = (
                f"{service}?serviceKey={key}"
                f"&numOfRows={rows}&pageNo={pageNo}"
                f"&year={year}&stdType={stdType}&st5Cd={code}&_type=json"
            )
            r = fetch_with_retry(url)
            if not r:
                continue
            try:
                items = r.json()["response"]["body"]["items"]["item"]
            except:
                continue

            for it in items:
                if it.get("diagCdNm") != "계":
                    continue
                records.append({
                    "Source":       "HIRA",
                    "Year":         int(year),
                    "Code":         code,
                    "DRG_Name":     subj,
                    "HIRA_Patients": int(it.get("ptntCnt", 0))
                })

    if not records:
        sys.exit("No HIRA Total data retrieved.")
    df = pd.DataFrame(records)
    return df.groupby(
        ["Source","Year","Code","DRG_Name"], as_index=False
    )["HIRA_Patients"].sum()

def fetch_kosis(base_dir):
    """DRG_Record + DRG_Code_matching → 연도별 DRG 환자 수"""
    rec  = pd.read_excel(os.path.join(base_dir, "DRG_Record.xlsx"), engine="openpyxl")
    cmap = pd.read_excel(os.path.join(base_dir, "DRG_Code_matching.xlsx"), engine="openpyxl")
    cmap = cmap[["질병군분류(2)","DRG(포괄수가제 행위 코드)"]].copy()
    cmap.columns = ["Code","DRG_Name"]
    cmap["Code"] = cmap["Code"].str.upper().str.strip()

    bcol  = rec.columns[1]
    clean = rec[~rec[bcol].astype(str).str.contains("소계|계", na=False)].copy()
    clean["Code"] = (
        clean[bcol].astype(str)
             .str.extract(r"^([A-Z]\d{3})")[0]
             .str.upper()
    )
    clean = clean[clean["Code"].isin(cmap["Code"])]

    years   = rec.columns[2:]
    df_long = clean.melt(
        id_vars=["Code"],
        value_vars=years,
        var_name="Year",
        value_name="KOSIS_Patients"
    )
    df_long["Year"] = df_long["Year"].astype(int)
    df_long["KOSIS_Patients"] = (
        df_long["KOSIS_Patients"]
               .astype(str).str.replace(",", "")
               .replace("-", "0").astype(int)
    )

    df_k = df_long.groupby(
        ["Year","Code"], as_index=False
    )["KOSIS_Patients"].sum()
    df_k = df_k.merge(cmap, on="Code", how="left")
    df_k["Source"]        = "DRG"
    df_k["HIRA_Patients"] = 0
    return df_k[["Source","Year","Code","DRG_Name","HIRA_Patients","KOSIS_Patients"]]

def main():
    base_dir = r"C:\Users\bkim2\Box\01. My Personal Folder\Market Analysis"

    df_hira = fetch_hira(base_dir)
    df_drg  = fetch_kosis(base_dir)

    # 합치기 & TotalPatients 계산
    df_all = pd.concat([df_hira, df_drg], ignore_index=True)
    total  = df_all.groupby(
        ["Source","Year","Code","DRG_Name"], as_index=False
    ).agg({
        "HIRA_Patients": "sum",
        "KOSIS_Patients": "sum"
    })
    total["TotalPatients"] = total["HIRA_Patients"] + total["KOSIS_Patients"]

    # 최종 저장
    out_file = os.path.join(
        base_dir, f"MarketData_{datetime.today().strftime('%Y-%m-%d')}.xlsx"
    )
    total.to_excel(out_file, index=False)
    print(f"✅ MarketData 파일 생성 완료: {out_file}")
    print(total.head(10))

if __name__ == "__main__":
    main()

