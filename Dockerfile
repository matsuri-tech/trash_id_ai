# ベースイメージとして軽量な Python イメージを使用
FROM python:3.9-slim

# 作業ディレクトリを作成
WORKDIR /app

# 依存関係ファイルをコピーしてインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードと JSON キーファイルをコピー
COPY main.py .
COPY m2m-core-08fd5de67ae7.json .

# Cloud Run では環境変数 PORT が設定されるため、デフォルトのポートを設定（必要に応じて変更）
ENV PORT 8080

# Cloud Run はリクエストハンドラを待機するため、もし main.py が定期実行（バッチ処理等）でなく HTTP サーバでない場合は
# [注意] この場合、コンテナ起動時に処理が実行され終了してしまう可能性があるため、Cloud Run のユースケースに合わせた起動方法に調整してください。
# ここでは例として main.py を直接実行する設定です。
CMD ["python", "main.py"]
