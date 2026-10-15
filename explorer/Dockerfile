FROM python:3.12-slim
RUN useradd -m -u 1000 app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=app:app . .
USER app
ENV PORT=7860 HF_HOME=/home/app/.cache/huggingface
EXPOSE 7860
CMD ["python", "app.py"]
