# ==========================================================
# CD BALANCE ENGINE — COLAB PRODUCTION SAFE BUILD
# ==========================================================

# ---------- 1. AUTHENTICATION (RUN FIRST ALWAYS) ----------
!rm -rf /root/.config/credentials 2>/dev/null

from google.colab import auth, drive
auth.authenticate_user()
drive.mount('/content/drive', force_remount=True)

import google.auth
from googleapiclient.discovery import build

creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
drive_service = build("drive", "v3", credentials=creds)


# ---------- 2. IMPORTS ----------
import pandas as pd
import io
from datetime import datetime
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


# ---------- 3. CONFIGURATION ----------
INPUT_FOLDER_ID  = "1osUmWdmeIuINxxPWKcJ83uPPY2IIqeJK"
OUTPUT_FOLDER_ID = "1J1ji0kth7l7waG6IMqyUbLMc75stv3Q3"

TRACKER_FILENAME = "CD_TRACKER.parquet"
MASTER_FILENAME  = "CD_MASTER.csv"


# ---------- 4. FILE LIST ----------
def list_all_excel_files(folder_id):
    all_files = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            fields="nextPageToken, files(id,name,modifiedTime,size)",
            pageSize=1000,
            pageToken=page_token
        ).execute()

        all_files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return all_files


# ---------- 5. DOWNLOAD ----------
def download_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh


# ---------- 6. UPLOAD / REPLACE ----------
def upload_or_replace(filename, data_bytes):

    media = MediaIoBaseUpload(io.BytesIO(data_bytes),
                              mimetype='application/octet-stream',
                              resumable=True)

    query = f"name='{filename}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    res = drive_service.files().list(q=query, fields="files(id)").execute()["files"]

    if res:
        drive_service.files().update(fileId=res[0]["id"], media_body=media).execute()
    else:
        drive_service.files().create(
            body={"name": filename, "parents": [OUTPUT_FOLDER_ID]},
            media_body=media
        ).execute()


# ---------- 7. TRACKER ----------
def load_tracker():

    query = f"name='{TRACKER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    res = drive_service.files().list(q=query, fields="files(id)").execute()["files"]

    if not res:
        print("First run — tracker will be created")
        return pd.DataFrame(columns=[
            "FileId","Modified","Size",
            "Client","MPN","VAN",
            "Last_Transaction_Date","Last_Balance"
        ])

    fh = download_file(res[0]["id"])
    return pd.read_parquet(fh)


# ---------- 8. HELPERS ----------
def extract_mpn(filename):
    parts = filename.replace(".xlsx","").split("-")
    return parts[2] if len(parts) >= 3 else None

def extract_client(xls):
    try:
        df = xls.parse("Details", header=None, usecols="D", nrows=3)
        return str(df.iloc[2,0]).strip()
    except:
        return None

def extract_van(xls):
    try:
        df = xls.parse("CD Statement", header=None, nrows=10, usecols="A:E")
        text = str(df.iloc[7,4])
        if "/" in text:
            return "ACKOG4" + text.split("/")[1].strip()
    except:
        return None
    return None

def detect_cd_table(xls):
    for sheet in xls.sheet_names:
        raw = xls.parse(sheet, header=None)
        for i in range(min(15, len(raw))):
            row = raw.iloc[i].astype(str).str.lower()
            if any("balance" in c for c in row) and any("date" in c for c in row):
                return xls.parse(sheet, header=i)
    return None


# ---------- 9. PROCESS ----------
files = list_all_excel_files(INPUT_FOLDER_ID)
print("Files detected:", len(files))

tracker = load_tracker()
tracker_dict = tracker.set_index("FileId").to_dict("index") if not tracker.empty else {}

new_rows = []

for file in files:

    fid = file["id"]
    name = file["name"]
    modified = file["modifiedTime"]
    size = file.get("size", None)

    if fid in tracker_dict:
        if tracker_dict[fid]["Modified"] == modified and tracker_dict[fid]["Size"] == size:
            new_rows.append(tracker_dict[fid])
            continue

    print("Reading:", name)

    fh = download_file(fid)
    xls = pd.ExcelFile(fh)

    df = detect_cd_table(xls)
    if df is None:
        continue

    bal_col = [c for c in df.columns if "balance" in str(c).lower()][0]
    date_col = [c for c in df.columns if "date" in str(c).lower()][0]

    df = df[df[bal_col].notna()]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    latest = df.sort_values(date_col, ascending=False).iloc[0]

    new_rows.append({
        "FileId": fid,
        "Modified": modified,
        "Size": size,
        "Client": extract_client(xls),
        "MPN": extract_mpn(name),
        "VAN": extract_van(xls),
        "Last_Transaction_Date": latest[date_col],
        "Last_Balance": latest[bal_col]
    })


# ---------- 10. MASTER ----------
combined = pd.DataFrame(new_rows)

master = combined.sort_values("Last_Transaction_Date", ascending=False)\
                 .drop_duplicates("MPN")

master["Last_Updated"] = datetime.now().strftime("%d-%b-%Y %I:%M %p")

master["Last_Transaction_Date"] = pd.to_datetime(
    master["Last_Transaction_Date"]
).dt.strftime("%Y-%m-%d")

master["Last_Balance"] = pd.to_numeric(master["Last_Balance"], errors="coerce")

master = master[
    ["Client","MPN","VAN","Last_Transaction_Date","Last_Balance","Last_Updated"]
]


# ---------- 11. SAVE ----------
upload_or_replace(MASTER_FILENAME, master.to_csv(index=False).encode("utf-8"))
upload_or_replace(TRACKER_FILENAME, combined.to_parquet(index=False))

print("\nSUCCESS — Incremental engine ready 🚀")
print("Next runs process only changed files.")
