import glob
import platform
# from db import mysql_connector as msc   # disabled for test mode
from datetime import datetime
import os
import argparse
import pandas as pd
import csv
from pathlib import Path

USE_DB = False

system = platform.system()
if system == "Windows":
    in_dir = Path("testFTP")
    cf_dir = Path("cnf/user.cnf.txt")
else:
    in_dir = Path("testFTP/")
    cf_dir = Path("cnf/user.cnf.txt")

db_scm = "cont"

def read_file(file, errFile):
    if file.suffix.lower() == ".csv":
        try:
            with file.open("r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                raw = [row for row in reader if row]
            return raw
        except FileNotFoundError as e:
            print(e)
    else:
        errFile.append([str(file), "Incorrect File Type"])

def read_xlsx(file, errFile):
    if file.suffix.lower() == ".xlsx":
        try:
            raw_df = pd.read_excel(
                file, sheet_name=0, header=None,
                keep_default_na=False, engine="openpyxl",
                usecols="A:G"
            )
            raw = raw_df.values.tolist()
            while raw and (raw[-1][0] == "" or raw[-1][0] is None):
                raw = raw[:-1]
            return raw
        except FileNotFoundError as e:
            print(e)
    else:
        errFile.append([str(file), "Incorrect File Type"])

def ck_time_format(time):
    try:
        return datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        if time.endswith('AM'):
            dt = datetime.strptime(time, '%m/%d/%y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
        if time.count(':') == 1:
            dt = datetime.strptime(time, '%m/%d/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
        else:
            dt = datetime.strptime(time, '%m/%d/%y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        return dt

with cf_dir.open("r") as f:
    s = f.read()
config = [line.split(",") for line in s.splitlines() if line]
config_uid = config[0][1]
config_pw = config[1][1]

ftp = in_dir
folder = "Upload"
insert_type = "Cont_Data"
fdir = list(ftp.glob(f"**/{folder}/{insert_type}/*.csv"))

headerList = ["Date_Time", "Temp", "UOM", "ProbeID", "SID", "Collector", "ProbeType", "dataFlag", "comment"]

print(f"found {len(fdir)} files to process: {fdir}")

try:
    for file in fdir:
        db_err = []
        print('processing file=%s' % file)
        uploadDate = datetime.today().strftime('%m%d%Y_%H%M%S_')
        fpath_base = os.path.dirname(os.path.dirname(str(file)))
        fpath_in = file
        fpath_err = os.path.join(fpath_base, 'ErrRpts', uploadDate + os.path.basename(file) + 'QcRpt.txt')
        fpath_out = os.path.join(fpath_base, 'UploadedRpts', 'Temperature', uploadDate + os.path.basename(file))
        fpath_eout = os.path.join(fpath_base, 'UploadedRpts', 'Temperature', 'Error', os.path.basename(file))
        delim = '\t'
        raw = read_file(fpath_in, db_err)
        header = raw[0]
        raw = raw[1:]

        if raw is not None and header == headerList:
            insDate = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            for i in range(len(raw)):
                try:
                    s_time = raw[i][0]
                    s_date = ck_time_format(s_time) if isinstance(s_time, str) else str(s_time)
                    file_name = os.path.basename(str(file))
                    user_name = os.path.basename(fpath_base)

                    print(f"[TEST MODE] would insert row {i}: {raw[i]}")

                except Exception as e:
                    msg = f"Row {i+2} error: {e}"
                    db_err += [[msg]]
                    print(msg)

            if len(db_err) < 1:
                s = 'All rows successfully processed'
                os.rename(fpath_in, fpath_out)
            else:
                s = '\n'.join([delim.join(row) if isinstance(row, list) else str(row) for row in db_err])
                os.rename(fpath_in, fpath_eout)

            with open(fpath_err, 'w') as f:
                f.write(s)
        else:
            print('File Error - Not uploaded')
            db_err += [[str(file),'File Error - Not uploaded.  Check file type column ordering and column names']]
            s = '\n'.join([delim.join([str(e) for e in row]) for row in db_err])
            with open(fpath_err, 'w') as f:
                f.write(s)

except FileNotFoundError as e:
    print(e)
