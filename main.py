import logging
import sys

# ログの初期化（タイムスタンプ付き、ログレベルINFO以上を標準出力へ出力）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

import requests
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
from google.cloud import storage

def get_api_token():
    """
    APIにログインしてトークンを取得する。
    """
    url = 'https://api.m2msystems.cloud/login'
    mail = "development+20211103@matsuri-tech.com"
    password = "rYGOOh9PgUxFhjhd"


    payload = {"email": mail, "password": password}

    print("[INFO] === get_api_token: 開始 ===")
    try:
        response = requests.post(url, json=payload)
        print("[DEBUG] Login response status code:", response.status_code)
        if response.status_code == 200:
            json_data = response.json()
            token = json_data.get('accessToken')
            if token:
                print("[INFO] トークン取得成功")
                print("[DEBUG] Token (first 20 chars):", token[:20], "...")
                print("[INFO] === get_api_token: End ===\n")
                return token
            else:
                print("[ERROR] トークンが見つかりません")
                print("[INFO] === get_api_token: End ===\n")
                return None
        else:
            print("[ERROR] トークン取得失敗", response.text)
            print("[INFO] === get_api_token: End ===\n")
            return None
    except requests.exceptions.RequestException as e:
        print("[ERROR] Exception in get_api_token:", e)
        print("[INFO] === get_api_token: 終了 ===\n")
        return None


def get_today_cleanings(token: str, target_photo_tour_id: str) -> list:
    """
    Bearer token を用いて、当日の清掃情報を取得。
    さらに photoTourId == target_photo_tour_id のものだけを抽出。
    """
    print("[INFO] === get_today_cleanings: 開始 ===")
    url = "https://api-cleaning.m2msystems.cloud/v4/operations/search"
    print("[DEBUG] Cleaning search URL:", url)

    # 現在のUTC時間を取得
    current_datetime = datetime.utcnow()

    # 日本時間に調整 (UTC + 9時間)
    japan_datetime = current_datetime + timedelta(hours=9)

    # 日本時間での今日の日付を取得
    today_str = japan_datetime.strftime('%Y-%m-%d')
    print("[DEBUG] Target date:", today_str)

    # ペイロードの作成
    payload = {
        "startDate": today_str,
        "endDate": today_str,
        "photoTourIds": [target_photo_tour_id]  # 引数で指定された photoTourId を絞り込む
    }
    print("[DEBUG] Request payload:", payload)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        print("[INFO] m2mにPOSTリクエストを行い、本日のゴミチェックツアーを取得します")
        response = requests.post(url, headers=headers, json=payload)
        print("[DEBUG] Response status code:", response.status_code)
        # ログが長くなる場合は部分的に表示
        print("[DEBUG] Response text (partial):", response.text[:300], "...")

        if response.status_code == 200:
            data = response.json()
            if not isinstance(data, list):
                print("[WARN] The response is not a list as expected. data =", data)
                print("[INFO] === get_today_cleanings: End ===\n")
                return []

            print(f"[DEBUG] 取得したゴミチェックのツアー数: {len(data)}")

            # フィルタリング済みなので、ここでの追加処理は不要
            cleaned_data = []
            for c in data:
                cleaned_data.append({
                    "id": c.get("id"),
                    "photoTourId": c.get("photoTourId"),
                    "placementId": c.get("placementId"),
                    "commonAreaName": c.get("commonAreaName"),
                    "status": c.get("status"),
                    "cleaningDate": c.get("cleaningDate"),
                    # 他にも必要なら追加
                })

            print("[INFO] === get_today_cleanings: 終了 ===\n")
            return cleaned_data
        else:
            print("[ERROR] Server returned error while getting cleanings:", response.text)
            print("[INFO] === get_today_cleanings: 終了 ===\n")
            return []
    except requests.exceptions.RequestException as e:
        print("[ERROR] Exception in get_today_cleanings:", e)
        print("[INFO] === get_today_cleanings: 終了 ===\n")
        return []






def get_collecting_and_commonarea_id():
    """
    明日の日付に基づいた曜日に対応するデータを取得する。
    """
    # 自分のプロジェクトIDを指定
    project_id = 'm2m-core'

    # BigQueryクライアントを作成（リージョンを指定）
    client = bigquery.Client(project=project_id, location='asia-northeast1')  # リージョンを指定

    # 実行するSQLクエリ
    sql = """
    WITH weekday_info AS (
      SELECT
        pl.placement_id,
        col.building_id,
        col.building_name,
        pl.common_area_id,
        pl.common_area_name,
        col.friday,
        col.monday,
        col.saturday,
        col.thursday,
        col.tuesday,
        col.wednesday,
        CASE
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 1 THEN 'friday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 2 THEN 'monday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 3 THEN 'tuesday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 4 THEN 'wednesday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 5 THEN 'thursday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 6 THEN 'friday'
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE() + INTERVAL 1 DAY) = 7 THEN 'saturday'
        END AS tomorrow_column
      FROM `m2m-core.su_wo.placement_relations` AS pl
      LEFT JOIN `m2m-core.su_wo.trash collection` AS col
        ON pl.property_id = col.building_id
    )

    SELECT
      placement_id,
      building_id,
      building_name,
      common_area_id,
      common_area_name,
      CASE
        WHEN tomorrow_column = 'friday' THEN friday
        WHEN tomorrow_column = 'monday' THEN monday
        WHEN tomorrow_column = 'tuesday' THEN tuesday
        WHEN tomorrow_column = 'wednesday' THEN wednesday
        WHEN tomorrow_column = 'thursday' THEN thursday
        WHEN tomorrow_column = 'saturday' THEN saturday
      END AS tomorrow_column_value
    FROM weekday_info
    WHERE common_area_id IS NOT NULL
    """

    # SQLクエリを実行
    query_job = client.query(sql)

    # 結果を取得
    results = query_job.result()

    # placement_data 辞書にデータを格納
    placement_data = {}

    for row in results:
        # placement_id をキーにしてデータを格納
        placement_data[row.placement_id] = {
            "placementId": row.placement_id,
            "common_area_id": row.common_area_id,
            "commonAreaName": row.common_area_name,
            "tomorrow_column_value": row.tomorrow_column_value,
            "building_id": row.building_id,
            "building_name": row.building_name,
        }

    return placement_data


def add_collecting_and_commonarea_id(cleanings: list, placement_data: dict) -> list:
    """
    清掃データに placement_records の情報を追加する。
    """
    for cleaning in cleanings:
        placement_id = cleaning.get("placementId")

        # placement_idに基づきplacement_dataから情報を取得
        placement_info = placement_data.get(placement_id, {})

        # placement_info が見つかった場合、関連する情報を清掃データに追加
        if placement_info:
            cleaning.update(placement_info)

        else:
            print(f"[WARN] placement_id {placement_id} not found in placement_data.")

    return cleanings

def filter_cleanings_by_tomorrow_column_value(cleanings: list) -> list:
    """
    enriched_cleanings リストから tomorrow_column_value が None のものを残し、
    それ以外のデータを省く。
    また、省かれたデータの building_id をデバッグ出力する。
    """
    filtered_cleanings = []
    excluded_cleanings = []  # To store excluded cleanings for debug output

    for cleaning in cleanings:
        if cleaning.get("tomorrow_column_value") is None:
            filtered_cleanings.append(cleaning)
        else:
            excluded_cleanings.append(cleaning)

    # Debug output for excluded properties
    if excluded_cleanings:
        print("[INFO] 明日回収があるため以下の物件が省かれました:")
        for cleaning in excluded_cleanings:
            commonAreaName = cleaning.get("commonAreaName")
            if commonAreaName:
                print(f"- {commonAreaName}")
            else:
                print("[DEBUG] Excluded cleaning with missing building_id")

    return filtered_cleanings,excluded_cleanings


def get_photo_tour_images_by_cleaning_id(token: str, cleaning_id: str) -> list:
    """
    指定した cleaning_id の Photo Tour 画像情報(複数) を取得。
    レスポンスは {"images": [...] } の構造を想定し、
    各要素の "url" をリスト化して返す。
    """

    url = f"https://api-cleaning.m2msystems.cloud/v3/photo_tour_images/by_cleaning_id/{cleaning_id}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        # print("[DEBUG] Status Code:", response.status_code)
        # print("[DEBUG] Response text (partial):", response.text[:300], "...")
        if response.status_code == 200:
            data = response.json()
            # data = { "images": [ { "id":..., "url":... }, ... ] }
            images_array = data.get("images", [])
            if not isinstance(images_array, list):
                print("[ERROR] 'images' field is not a list. data =", data)
                return []

            urls = []
            for img_obj in images_array:
                url_value = img_obj.get("url")
                if url_value:
                    urls.append(url_value)

            return urls
        else:

            return []
    except requests.RequestException as e:
        print("[ERROR] Request exception:", e)
        return []


def add_images_to_cleaning(token: str, cleaning_data: dict) -> dict:
    """
    1件の清掃データ(dict)を受け取り、対応する複数画像URLを取得し、
    "urls" フィールドとして格納して返す。
    """
    cid = cleaning_data.get("id")
    if not cid:
        print("[WARN] cleaning_data に 'id' が無いため画像取得不可.")
        cleaning_data["urls"] = []
        return cleaning_data

    # 複数URLをリストで取得
    image_urls = get_photo_tour_images_by_cleaning_id(token, cid)
    cleaning_data["urls"] = image_urls  # リストをそのまま詰める

    return cleaning_data


def enrich_multiple_cleanings_with_images(token: str, cleaning_list: list) -> list:
    """
    清掃データ(複数) に対して、全ての画像URLを付与。
    "urls" キーに [url1, url2, ...] が入る形で返す。
    """
    enriched = []
    for c in cleaning_list:
        new_c = add_images_to_cleaning(token, c)
        enriched.append(new_c)
    return enriched



# 1) クラス判定を行うエンドポイント (Trash_id_AI)
URL_JUDGE = "https://us-central1-m2m-core.cloudfunctions.net/Trash_id_AI"

# 2) 判定結果が「ストッカー」だった場合に呼び出すエンドポイント
URL_STOCKER = "https://us-central1-m2m-core.cloudfunctions.net/Trash_stocker_id_AI"

# 3) 判定結果が「ゴミ庫」だった場合に呼び出すエンドポイント
URL_PLACE = "https://us-central1-m2m-core.cloudfunctions.net/Trash_place_id_AI"

def judge_trash(image_urls: list) -> list:
    """
    複数の画像URLと個別IDを受け取り、判定を行う。
    画像URLが空の場合は判定をスキップするが、空の場合にも結果を出力する。
    """
    results = []

    for image_url in image_urls:
        if not image_url:  # 画像URLが空の場合
            results.append({
                "url": "",
                "object": "画像無し",  # 画像がない場合
                "result": "画像無し",  # 画像がない場合
            })
            continue  # 次の画像へ

        # (A) 1回目: ゴミ庫/ストッカーを判定
        payload = {"urls": [image_url]}
        response_judge = requests.post(URL_JUDGE, json=payload)
        response_judge_data = response_judge.json()

        # 判定結果が空の場合（想定外のレスポンス）
        results_first = response_judge_data.get("results", [])
        if not results_first:
            results.append({
                "url": image_url,
                "result": "判定不可 (1回目の結果なし)",
            })
            continue  # 次の画像へ

        # 1件だけ取り出し
        first_item = results_first[0]
        first_result_label = first_item.get("result", "")  # 例: "ストッカー" or "ゴミ庫"

        # 2回目の判定（ストッカー or ゴミ庫）
        final_result = "不明"

        if first_result_label == "ストッカー":
            resp = requests.post(URL_STOCKER, json=payload)
            resp_data = resp.json()
            results_second = resp_data.get("results", [])
            if results_second:
                second_label = results_second[0].get("result", "")
                if second_label.endswith("〇"):
                    final_result = "〇"
                elif second_label.endswith("×"):
                    final_result = "×"
                else:
                    final_result = second_label  # 想定外の場合そのまま保持
            else:
                final_result = "判定不可 (2回目のストッカー結果なし)"

        elif first_result_label == "ゴミ庫":
            resp = requests.post(URL_PLACE, json=payload)
            resp_data = resp.json()
            results_second = resp_data.get("results", [])
            if results_second:
                second_label = results_second[0].get("result", "")
                if second_label.endswith("〇"):
                    final_result = "〇"
                elif second_label.endswith("×"):
                    final_result = "×"
                else:
                    final_result = second_label
            else:
                final_result = "判定不可 (2回目のゴミ庫結果なし)"

        else:
            final_result = f"予想外のラベル: {first_result_label}"

        # 結果をリストに追加
        results.append({
            "url": image_url,
            "object": first_result_label,
            "result": final_result,
        })

    return results


def get_prefeture(data):
    import json
    from google.cloud import bigquery

    # BigQueryクライアントの初期化（プロジェクトIDを指定）
    client = bigquery.Client(project="m2m-core")

    # 内部関数：1件のレコードを処理する
    def process_record(record):
        # 引数recordからcommon_area_idを取得
        common_area_id = record.get("common_area_id")
        if not common_area_id:
            raise ValueError("recordにcommon_area_idが含まれていません。")

        # SQLクエリ：該当するcommon_area_idのレコードの都道府県部分を抽出
        query = f"""
        SELECT 
          REGEXP_EXTRACT(address, r'^(.*?[都道府県])') AS prefecture
        FROM `m2m-core.m2m_cleaning_prod.placement_records`
        WHERE common_area_id = '{common_area_id}'
        LIMIT 1
        """

        # クエリ実行
        query_job = client.query(query)
        results = query_job.result()

        # 結果が見つかった場合、その都道府県部分を取得
        prefecture = None
        for row in results:
            prefecture = row.prefecture
            break

        # recordにprefectureキーを追加
        record["prefecture"] = prefecture
        return record

    # 引数がリストの場合、各レコードを処理する
    if isinstance(data, list):
        processed = [process_record(record) for record in data]
    # 辞書の場合はそのまま処理
    elif isinstance(data, dict):
        processed = process_record(data)
    else:
        raise ValueError("dataは辞書または辞書のリストでなければなりません。")

    # JSON形式で結果を出力（日本語をそのまま表示）
    json_output = json.dumps(processed, ensure_ascii=False, indent=2)
    print(json_output)

    return processed


import json
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

# Google Sheets APIの設定
SERVICE_ACCOUNT_FILE = 'm2m-core-d84008adb45d.json'  # アップロードしたJSONキーのパス
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# サービスアカウントを使用して認証
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Google Sheetsのサービスオブジェクト
service = build('sheets', 'v4', credentials=creds)

# スプレッドシートのID
spreadsheet_id = '1cV_8DH4wLZu7--J9vxIH0rFf7upKa1cKACyDNaONxp0'  # 使用するスプレッドシートのID

# 日本時間を取得する関数
def get_japan_time():
    current_datetime = datetime.now()
    japan_datetime = current_datetime + timedelta(hours=9)  # UTC+9時間で日本時間に調整
    return japan_datetime.strftime('%Y-%m-%d %H:%M:%S')  # 日本時間のフォーマットで返す

# Google Sheetsにデータを書き込む関数
def write_to_sheet(data, sheet_name="判定一覧"):
    """
    final_resultのデータを指定したGoogle Sheetsのシートに書き込む関数
    """
    # 書き込む範囲をA2からF列に設定
    range_name = f'{sheet_name}!A2:F'  # 書き込む範囲をA2からF列に変更

    # ヘッダーの設定
    header = ['cleaning_id', 'name', 'result', 'object', 'picture_url', 'm2m_url']
    values = [header]

    # `data` から値を取り出してリストに追加
    for item in data:
        cleaning_id = item.get('id', '')  # Cleaning ID
        name = item.get('commonAreaName', '')  # Common Area Name

        # 各judgmentのデータを処理
        for judgment in item.get('judgments', []):
            result_value = judgment.get('result', '')  # '〇' または '×' をそのまま取得
            m2m_url = f"https://manager-cleaning.m2msystems.cloud/operations/{cleaning_id}"  # URLを生成

            row = [
                cleaning_id,             # Cleaning ID
                name,                    # Name
                result_value,            # Result ('〇' または '×')
                judgment.get('object', ''),  # Object (判定結果: "ストッカー" または "ゴミ庫")
                judgment.get('url', ''),  # URL
                m2m_url
            ]
            values.append(row)

    try:
        # 範囲をクリア
        clear_range = f'{sheet_name}!A2:H'
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=clear_range).execute()
        print("[INFO] シートの範囲をクリアしました。")

        # Sheets APIにデータを書き込む
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption="RAW", body={"values": values}
        )
        response = request.execute()
        print("[INFO] スプレッドシートにデータを書き込みました。")
        print(response)

        # ステータスセルに「判別終了」を設定
        update_status_range = "判定一覧!H3"
        update_status_values = [["判別終了"]]
        request_status = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=update_status_range,
            valueInputOption="RAW", body={"values": update_status_values}
        )
        request_status.execute()  # 実行
        print("[INFO] ステータスセルに「判別終了」を設定しました。")

        # 実行時間を日本時間で取得
        execution_time = get_japan_time()

        # 実行時間をシートに追加
        update_time_range = "判定一覧!H4"
        update_time_values = [[execution_time]]
        request_time = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=update_time_range,
            valueInputOption="RAW", body={"values": update_time_values}
        )
        request_time.execute()  # 実行
        print(f"[INFO] 実行時間をシートに追加しました: {execution_time}")

    except Exception as e:
        print(f"[ERROR] Google Sheetsへの書き込みに失敗しました: {e}")




import json
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Google Sheets APIの設定
SERVICE_ACCOUNT_FILE = 'm2m-core-d84008adb45d.json'  # サービスアカウントのJSONキーのパス
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# サービスアカウントを使用して認証
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Google Sheetsのサービスオブジェクト
service = build('sheets', 'v4', credentials=creds)

# スプレッドシートのID
spreadsheet_id = '1cV_8DH4wLZu7--J9vxIH0rFf7upKa1cKACyDNaONxp0'  # 使用するスプレッドシートのID

# 日本時間を取得する関数
def get_japan_time():
    current_datetime = datetime.now()
    japan_datetime = current_datetime + timedelta(hours=9)  # UTC+9時間で日本時間に調整
    return japan_datetime.strftime('%Y-%m-%d %H:%M:%S')  # 日本時間のフォーマットで返す

# Google Sheetsにデータを書き込む関数
def write_to_sheet(data, sheet_name="判定一覧"):
    """
    Write the results of the cleaning data to the specified Google Sheets sheet.
    """
    if not data:
        print("[INFO] No data to write to the sheet.")
        return

    range_name = f'{sheet_name}!A2:F'
    header = ['cleaning_id', 'name', 'result', 'object', 'picture_url', 'm2m_url']
    values = [header]

    for item in data:
        cleaning_id = item.get('id', '')
        name = item.get('commonAreaName', '')
        if not item.get('judgments'):
            row = [
                cleaning_id,
                name,
                '',
                '',
                '',
                f"https://manager-cleaning.m2msystems.cloud/operations/{cleaning_id}"
            ]
            values.append(row)
        else:
            for judgment in item.get('judgments', []):
                result_value = judgment.get('result', '')
                m2m_url = f"https://manager-cleaning.m2msystems.cloud/operations/{cleaning_id}"
                row = [
                    cleaning_id,
                    name,
                    result_value,
                    judgment.get('object', ''),
                    judgment.get('url', ''),
                    m2m_url
                ]
                values.append(row)
    try:
        clear_range = f'{sheet_name}!A2:H'
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=clear_range
        ).execute()
        print("[INFO] シートの範囲をクリアしました。")

        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": values}
        )
        response = request.execute()
        print("[INFO] スプレッドシートにデータを書き込みました。")
        print("Response:", response)

        update_status_range = "判定一覧!H3"
        update_status_values = [["判別終了"]]
        request_status = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_status_range,
            valueInputOption="RAW",
            body={"values": update_status_values}
        )
        request_status.execute()
        print("[INFO] ステータスセルに「判別終了」を設定しました。")

        execution_time = get_japan_time()
        update_time_range = "判定一覧!H4"
        update_time_values = [[execution_time]]
        request_time = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_time_range,
            valueInputOption="RAW",
            body={"values": update_time_values}
        )
        request_time.execute()
        print(f"[INFO] 実行時間をシートに追加しました: {execution_time}")
    except Exception as e:
        print("[ERROR] Google Sheetsへの書き込みに失敗しました:")
        import traceback
        traceback.print_exc()



from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

def insert_results_to_bigquery(results):
    """
    結果データ (results: リスト形式の辞書) を BigQuery の指定テーブルにインサートする関数。
    table_id は "project.dataset.table" の形式で指定します。
    """
    table_id = "m2m-core.su_wo.test_trash_ai_result"
    client = bigquery.Client()

    # 結果データに日付・時刻情報を追加
    current_datetime = datetime.now()
    japan_datetime = current_datetime + timedelta(hours=9)
    current_date = japan_datetime.strftime('%Y-%m-%d')
    current_time = japan_datetime.strftime('%H:%M:%S')
    jst = pytz.timezone('Asia/Tokyo')
    now_jst = datetime.now(jst)
    current_timestamp = now_jst.timestamp()

    for result in results:
        result.pop('tomorrow_column_value', None)
        result.pop('status', None)
        result.pop('urls', None)
        result.pop('building_name', None)
        result.pop('judgments', None)
        result.pop('commonAreaName', None)
        result.pop('cleaningDate', None)
        result.pop('photoTourId', None)
        result.pop('id', None)
        result.pop('building_id', None)

        result["date"] = current_date
        result["time"] = current_time
        result["timestamp"] = current_timestamp

        result_value = result.get("result")
        if result_value == "〇":
            result["result"] = 1
        elif result_value == "×":
            result["result"] = 0
        else:
            result["result"] = None

        result["unique_id"] = result.get("unique_id", None)
        result["cleaning_id"] = result.get("cleaning_id", None)
        result["name"] = result.get("name", None)
        result["placement_id"] = result.get("placementId", None)
        result["object"] = result.get("object", None)
        result["common_area_id"] = result.get("common_area_id", None)
        result.pop('placementId', None)

        print("[DEBUG] Insert row:", result)

    try:
        errors = client.insert_rows_json(table_id, results)
        if errors:
            print("[ERROR] BigQueryへのインサート中にエラーが発生しました:", errors)
        else:
            print("[INFO] BigQueryに正常にインサートされました。")
    except Exception as e:
        print("[ERROR] BigQueryへのデータ挿入処理で例外が発生しました:")
        import traceback
        traceback.print_exc()



from google.cloud import bigquery
from datetime import datetime
import pytz
import uuid

def insert_results_to_bigquery(results):
    """
    結果データ (results: リスト形式の辞書) を BigQuery の指定テーブルにインサートする関数。
    table_id は "project.dataset.table" の形式で指定します。
    """

    # BigQueryクライアントの作成。認証は環境変数または明示的なキー指定により行う
    client = bigquery.Client()
    table_id = "m2m-core.su_wo.trash_ai_judging_result"

    # 結果データの変換
    rows_to_insert = []
    for result in results:
        # unique_idをランダムに生成
        unique_id = str(uuid.uuid4())
        
        # judgmentsからデータを抽出し、スキーマに合わせて変換
        if result["judgments"]:
            for judgment in result["judgments"]:
                # timestampを日本時間に設定
                timestamp = datetime.now(pytz.timezone('Asia/Tokyo'))
                date = timestamp.date().isoformat()  # YYYY-MM-DD形式
                time = timestamp.strftime("%H:%M")  # HH:MM形式に変更

                row = {
                    "unique_id": unique_id,
                    "cleaning_id": result["id"],  # idをcleaning_idとして使用
                    "name": result["commonAreaName"],
                    "placement_id": result["placementId"],
                    "result": 1 if judgment["result"] == "×" else 0,  # ×は1、それ以外は0
                    "url": judgment["url"],
                    "object": judgment["object"],
                    "common_area_id": result["common_area_id"],
                    "date": date,
                    "time": time,
                    "timestamp": timestamp.isoformat()  # 現在のタイムスタンプ
                }
                rows_to_insert.append(row)
        else:
            # judgmentsが空の場合でも1行挿入する場合の処理
            timestamp = datetime.now(pytz.timezone('Asia/Tokyo'))
            date = timestamp.date().isoformat()  # YYYY-MM-DD形式
            time = timestamp.strftime("%H:%M")  # HH:MM形式に変更

            row = {
                "unique_id": unique_id,
                "cleaning_id": result["id"],  # idをcleaning_idとして使用
                "name": result["commonAreaName"],
                "placement_id": result["placementId"],
                "result": None,  # judgmentsがない場合はresultもNone
                "url": None,
                "object": None,
                "common_area_id": result["common_area_id"],
                "date": date,
                "time": time,
                "timestamp": timestamp.isoformat()
            }
            rows_to_insert.append(row)

    # BigQueryにデータをインサート
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("BigQueryへのインサート中にエラーが発生しました:", errors)
    else:
        print("BigQueryに正常にインサートされました。")



def arrange_making_tour_data(data):
    """
    judgments 内に result が "〇" または 1 のレコードのみを抽出し、
    該当レコードの common_area_id と prefecture のペアを辞書形式のリストで返す関数。
    例:
      [
        { "common_area_id": "01H3DM2893SJ7W3ZN2R718JS33", "prefecture": "東京都" },
        { "common_area_id": "01J92P2XA103MG684VS1NZBD5E", "prefecture": "大阪府" }
      ]
    """
    filtered_records = []
    for record in data:
        judgments = record.get("judgments", [])
        for judgment in judgments:
            if judgment.get("result") == "〇" or judgment.get("result") == 1:
                filtered_records.append(record)
                break

    common_area_info = []
    for record in filtered_records:
        common_area_id = record.get("common_area_id")
        prefecture = record.get("prefecture")
        if common_area_id:
            common_area_info.append({
                "common_area_id": common_area_id,
                "prefecture": prefecture
            })

    print("抽出された common_area_info:")
    print(common_area_info)
    
    return common_area_info


def making_tour(common_area_info):
    """
    各 common_area_info のレコードごとに API リクエストを実行し、
    prefecture が "大阪府" の場合、cleaners の値を
    ["7903b3e8-923f-4f87-b5bb-3fee0a754d7c"] に変更する。
    それ以外はデフォルトの ["71461896-b63f-49de-a522-7b86671b6bbc"] を使用する。
    """
    try:
        print("makingTour 実行開始")
        token = get_api_token()
        if not token:
            print("トークンを取得できませんでした。")
            raise Exception("トークン取得失敗")
        print("トークン取得成功:", token)
        
        print("抽出された common_area_info の件数:", len(common_area_info))
        
        # 明日の日付を JST で取得（yyyy-MM-dd形式）
        import pytz
        tz = pytz.timezone('Asia/Tokyo')
        tomorrow = datetime.now(tz) + timedelta(days=1)
        cleaning_date = tomorrow.strftime("%Y-%m-%d")
        print("明日の日付:", cleaning_date)
        
        # 各 common_area_info のレコードごとに API リクエストを実行
        for idx, info in enumerate(common_area_info, start=2):
            common_area_id = info.get("common_area_id")
            prefecture = info.get("prefecture")
            if not common_area_id:
                print(f"commonAreaId が空のためスキップ (idx: {idx})")
                continue
            print(f"処理中: commonAreaId = {common_area_id}, prefecture = {prefecture}")
            note = (
                "ストッカー内・ゴミ庫内の清掃をしてください。水栓が使用できる場合は使用して、"
                "ストッカー底面やゴミ庫の床の清掃もお願いします"
            )
            api_url = "https://api-cleaning.m2msystems.cloud/v3/cleanings/create_with_placement"
            
            # prefecture によって cleaners を決定する
            cleaners = ["71461896-b63f-49de-a522-7b86671b6bbc"]
            if prefecture == "大阪府":
                cleaners = ["7903b3e8-923f-4f87-b5bb-3fee0a754d7c"]

            payload = {
                "placement": "commonArea",
                "commonAreaId": common_area_id,
                "listingId": "",
                "cleaningDate": cleaning_date,
                "note": note,
                "cleaners": cleaners,
                "submissionId": "",
                "photoTourId": "63534d08-f83b-4b3d-bd0c-0660be6ea3cb"
            }
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json"
            }
            try:
                response = requests.post(api_url, headers=headers, json=payload, timeout=360)
                print(f"APIレスポンス (common_area_id={common_area_id}):", response.text)
            except Exception as error:
                print("APIリクエストでエラーが発生しました:", str(error))
        print("makingTour 処理終了")
    except Exception as e:
        print("makingTour エラーが発生しました:", str(e))


def main():
    print("プログラムが開始されました。")

    # 例: トークンの取得
    token = get_api_token()
    if not token:
        print("[ERROR] トークン取得失敗")
        return

    # 例: 当日の清掃データの取得
    target_photo_tour_id = "0a8b54c9-1d99-460b-9085-4bdfe56af9ce"
    filtered_cleanings = get_today_cleanings(token, target_photo_tour_id)
    print(f"[INFO] 本日のゴミ状況チェックツアーの数: {len(filtered_cleanings)}")

    if filtered_cleanings:
        print("[INFO] 本日のゴミ状況チェックツアーの詳細:")
        for cleaning in filtered_cleanings:
            print(cleaning)
    else:
        print("[ERROR] 本日のゴミ状況チェックツアーが見つかりませんでした")
        return

    # placement_records の取得
    placement_data = get_collecting_and_commonarea_id()
    # 各清掃データに placement 情報を追加
    enriched_cleanings = add_collecting_and_commonarea_id(filtered_cleanings, placement_data)
    # 明日の日付に基づいて tomorrow_column_value が None でないデータを除外
    classificated_cleanings = filter_cleanings_by_tomorrow_column_value(enriched_cleanings)
    filtered_cleanings = classificated_cleanings[0]
    excluded_cleanings = classificated_cleanings[1]

    print(f"[INFO] 明日回収作業が行われないゴミチェックツアーの数: {len(filtered_cleanings)}")
    print(filtered_cleanings)

    # 複数の清掃データに対して画像URLを追加
    ai_data = enrich_multiple_cleanings_with_images(token, filtered_cleanings)
    if not ai_data:
        print("[ERROR] No AI data available after enrichment.")
        return

    total_images = sum(len(cleaning.get("urls", [])) for cleaning in ai_data)
    print(f"[INFO] 取得した画像は {total_images}枚です")

    print("[INFO] === ai_data の詳細 ===")
    for index, item in enumerate(ai_data):
        print(f"[INFO] --- Item {index + 1} ---")
        print(json.dumps(item, ensure_ascii=False, indent=4))
        print("[INFO] -------------------------")

    # AI判定を実施し、各清掃データに judgments を追加
    result = []
    for cleaning in ai_data:
        image_urls = cleaning.get("urls", [])
        commonarea_name = cleaning.get("commonAreaName", "不明")
        if not image_urls:
            print(f"[INFO] {commonarea_name}の画像がありませんでした。")
            cleaning["judgments"] = []
            result.append(cleaning)
            continue

        print(f"[INFO] {commonarea_name}の画像を判定中")
        judgments = []
        for url in image_urls:
            judge_result = judge_trash([url])
            if judge_result:
                judgments.extend(judge_result)
        cleaning["judgments"] = judgments if judgments else []
        result.append(cleaning)

    # 各レコードに prefecture 情報を追加
    result = [get_prefeture(record.copy()) for record in result]
    print("[INFO] result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("[INFO] スプレッドシートに出力します")
    write_to_sheet(result)

    print("[INFO] BQにインサートします")
    insert_results_to_bigquery(result)

    # making_tour 用の common_area_info（common_area_id と prefecture のペア）を抽出
    common_area_info = arrange_making_tour_data(result)

    print( "[INFO]common_area_info:")
    print(common_area_info)
    
    # common_area_info のみを引数として making_tour を実行
    # ※ この処理は、〇の判定が1件でも出た物件に対してツアーを作成します
    # making_tour(common_area_info)

    print("[INFO] すべての処理が完了しました")



from flask import Flask, request, jsonify
import os

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    # GASからのリクエストボディ(JSON)を取得（必要に応じて利用）
    payload = request.get_json(silent=True) or {}
    print("[INFO] Received payload:", payload)
    
    # メイン処理を実行
    main()
    
    # JSON形式でレスポンスを返す
    return jsonify({'message': '処理が完了しました。'})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
