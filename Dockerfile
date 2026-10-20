FROM python:3.14-alpine
WORKDIR /app
COPY requirements.txt hf_index-0.1.0-py3-none-any.whl ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py config.py grid.py ./
EXPOSE 5000
CMD ["gunicorn", "-b", "0.0.0.0:5000", "--workers", "2", "app:app"]
