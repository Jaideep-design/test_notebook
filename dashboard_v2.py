# -*- coding: utf-8 -*-
"""
Created on Thu Jun 19 12:41:26 2025

@author: Admin
"""

# Updated version of your script with Google Spreadsheet support for comments
from datetime import datetime
import os
import streamlit as st
import pandas as pd
import io
import json
import base64
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = r"C:\Users\Admin\Desktop\solar-ac-customer-mapping-905e295dd0db.json"
CSV_FILE_ID = '17o6xqWHYCTDCUAcRO-dLKGzmNPTuz___'
CSV_FILE_ID_2 = '17HdsQxLB6GlDuxd5yYLKPOlw9JrbWl40'
COMMENTS_SHEET_ID = '1vqk13WA77LuSl0xzb54ESO6GSUfqiM9dUgdLfnWdaj0'
COMMENTS_SHEET_NAME = 'solarac_Comments_log'
FOLDER_ID = '1QxSN4cOPxXwjIqvenJpCTiZNPPGEl6GV'

COLUMNS_RAW = ['Topic', 'timestamp', 'PV_kWh', 'OP_kWh', 'BATT_V_min',
               'ac_on_duration_h', 'AC_ROOM_TEMP_avg', 'avg_?T', 'unfiltered_transitions_to_level_0', 'non_acload_avg_W']
COLUMNS_LATEST = ['Topic', 'BATT_V_min', 'BATT_V', 'BATT_TYPE', 'MAX_CHG_I']

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# === GOOGLE AUTH ===

# Decode and load credentials from Streamlit Secrets
key_json = base64.b64decode(st.secrets["gcp_service_account"]["key_b64"]).decode("utf-8")
service_account_info = json.loads(key_json)

creds = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

# creds = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES
# )
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# Authenticate
gc = gspread.authorize(creds)
sh = gc.open_by_key(COMMENTS_SHEET_ID)
worksheet = sh.worksheet(COMMENTS_SHEET_NAME)

# === UTILITIES ===
def download_csv(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

def load_comments():
    try:
        data = worksheet.get_all_records()
        print("Raw data from sheet:", data)

        if not data:
            print("No data found in sheet.")
        
        df_comments = pd.DataFrame(data)

        # Strip whitespace from column names
        df_comments.columns = df_comments.columns.str.strip()

        print("Stripped columns:", df_comments.columns.tolist())
        print("DataFrame created:", df_comments.head())

        # Ensure required columns are present
        for col in ["Topic", "Timestamp", "Comment"]:
            if col not in df_comments.columns:
                print(f"Missing column: {col}")
                df_comments[col] = None

        return df_comments[["Topic", "Timestamp", "Comment"]]
    except Exception as e:
        print(f"Failed to load comments from sheet: {e}")
        return pd.DataFrame(columns=["Topic", "Timestamp", "Comment"])

def add_comment(topic, comment_text):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        worksheet.append_row([topic, timestamp, comment_text.strip()])
        print("Comment added successfully.")
    except Exception as e:
        print(f"Error adding comment: {e}")

# === DATA PROCESSING ===
def process_data(df1_raw, df2_latest):
    df1 = df1_raw[COLUMNS_RAW].copy()
    df2 = df2_latest[COLUMNS_LATEST].copy()

    df1['timestamp'] = pd.to_datetime(df1['timestamp'], format='%d-%m-%Y')
    latest_dates = df1.groupby('Topic')['timestamp'].max().reset_index()
    latest_dates.columns = ['Topic', 'latest_date']
    df1 = df1.merge(latest_dates, on='Topic')
    df_last7 = df1[df1['timestamp'] >= df1['latest_date'] - pd.Timedelta(days=6)]

    avg_last7 = df_last7.groupby('Topic')[[
        'PV_kWh', 'OP_kWh', 'ac_on_duration_h',
        'AC_ROOM_TEMP_avg', 'avg_?T', 'unfiltered_transitions_to_level_0', 'non_acload_avg_W']].mean().reset_index()

    avg_last7.iloc[:, 1:] = avg_last7.iloc[:, 1:].round(0)
    avg_last7 = avg_last7.merge(latest_dates, on='Topic', how='inner')
    avg_last7 = avg_last7.rename(columns={
        'AC_ROOM_TEMP_avg': 'AC_RTEMP_avg',
        'latest_date': 'timestamp',
        'avg_?T': 'avg_delta_temp',
        'unfiltered_transitions_to_level_0': 'Trips'
    })
    avg_last7['timestamp'] = avg_last7['timestamp'].dt.strftime('%Y-%m-%d')

    result_df = df2.merge(avg_last7, on='Topic', how='left')
    result_df = result_df[~result_df.loc[:, result_df.columns.difference(['Topic'])].isna().all(axis=1)]
    result_df = result_df.rename(columns={
        'BATT_V_min': 'B_V_min',
        'BATT_V': 'B_V',
        'BATT_TYPE': 'B_TYPE'
    })
    return result_df
# %%


# === STREAMLIT APP ===
st.set_page_config(page_title="Ecozen Solar AC", layout="wide")
st.title("📊 Dashboard")

if st.button("🔄 Refresh & Process Data"):
    with st.spinner("Fetching CSVs from Google Drive..."):
        df_raw = download_csv(CSV_FILE_ID)
        df_latest = download_csv(CSV_FILE_ID_2)
        final_df = process_data(df_raw, df_latest)
        st.session_state.final_df = final_df
    st.success("✅ Data refreshed and processed!")

if "final_df" in st.session_state:
    st.subheader("📋 Last 7 Day Average Data")
    all_topics = st.session_state.final_df['Topic'].unique().tolist()
    topic_options = ["All"] + sorted(all_topics)
    selected_topic = st.selectbox("Select a device (Topic):", topic_options)

    if selected_topic == "All":
        filtered_df = st.session_state.final_df
    else:
        filtered_df = st.session_state.final_df[st.session_state.final_df['Topic'] == selected_topic]

    comments_df = load_comments()

    if selected_topic != "All":
        topic_comments = comments_df[comments_df["Topic"] == selected_topic]

        st.dataframe(filtered_df, use_container_width=True)
        st.subheader("📝 Comments")
        st.markdown(f"**Previous Comments for Topic: `{selected_topic}`**")
        if not topic_comments.empty:
            topic_comments['Timestamp'] = pd.to_datetime(topic_comments['Timestamp'])
            st.dataframe(topic_comments.sort_values("Timestamp", ascending=False), use_container_width=True)
        else:
            st.info("No comments yet for this Topic.")

        new_comment = st.text_area("Add a new comment:")
        if st.button("Submit Comment"):
            if new_comment.strip():
                add_comment(selected_topic, new_comment)
                st.success("Comment added!")
                st.rerun()
            else:
                st.warning("Please enter a comment before submitting.")
    else:
        if not comments_df.empty:
            comments_df["Timestamp"] = pd.to_datetime(comments_df["Timestamp"])
            latest_comments = (
                comments_df.sort_values("Timestamp", ascending=False)
                .drop_duplicates(subset=["Topic"])
                [["Topic", "Comment"]]
            )
            filtered_df = filtered_df.merge(latest_comments, on="Topic", how="left")
            cols = filtered_df.columns.tolist()
            if "Comment" in cols:
                topic_index = cols.index("Topic")
                cols.insert(topic_index + 1, cols.pop(cols.index("Comment")))
                filtered_df = filtered_df[cols]
        else:
            filtered_df["Comment"] = None

        st.dataframe(filtered_df, use_container_width=True)
else:
    st.info("Click '🔄 Refresh & Process Data' to begin.")
