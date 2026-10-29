FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY service.py .
COPY model ./model

EXPOSE 3000
CMD ["bentoml", "serve", "service:RiskService", "--port", "3000"]