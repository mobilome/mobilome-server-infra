# 使用官方 Python 3.12 作为基础镜像
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY app.py .
COPY config.json .

# 设置环境变量，Flask 运行
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=19998

# 暴露端口
EXPOSE 19998

# 默认启动命令
CMD ["flask", "run"]
