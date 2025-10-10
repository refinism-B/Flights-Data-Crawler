# 使用官方 Python 基礎映像檔
FROM python:3.11.13-bookworm

# 設定工作目錄
WORKDIR /app

# 設定時區和語言編碼
ENV TZ=Asia/Taipei
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# 更新Linux的安裝套件apt-get，並安裝git、curl
RUN apt-get update && apt-get install -y \
    git \
    curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 升級python的pip套件管理工具
RUN python -m pip install --upgrade pip

# 將python套件清單複製進container
COPY requirements.txt .

# 根據python套件清單安裝所需的第三方套件
RUN pip install --no-cache-dir -r requirements.txt

# 將應用程式檔案複製到容器中
COPY . /app

# 設置硬碟掛載點
VOLUME /app/scripts

# 設定環境變數
ENV PYTHON_ENV=production
ENV APP_PORT=3000
EXPOSE 3000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
    CMD curl -f http://localhost:3000/ || exit 1

# 啟動指令
CMD ["python", "app.py"]