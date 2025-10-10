from flask import Flask, jsonify
import subprocess
import os

app = Flask(__name__)

# 定義一個 HTTP GET 端點
@app.route('/run-script', methods=['GET'])
def run_script():
    try:
        # 執行你的 Python 程式
        subprocess.run(['python', '/app/AllCorp_ListCrawler.py'], check=True)
        return jsonify({"status": "success", "message": "Script executed successfully!"}), 200
    except Exception as e:
        # 如果執行失敗，返回錯誤訊息
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # 從環境變數中讀取埠號，默認為 3000
    port = int(os.environ.get("PORT", 3000))
    app.run(host='0.0.0.0', port=port)
