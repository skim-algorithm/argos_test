FROM python:3.10-slim

WORKDIR /app
RUN apt-get update

# ta-lib 빌드를 위한 패키지 추가
RUN apt-get install --yes --no-install-recommends gcc build-essential wget

RUN python3 -m venv .venv
RUN . .venv/bin/activate

RUN pip install --upgrade pip

# ta-lib 사용을 위한 설치
COPY scripts scripts
RUN ["chmod", "+x", "/app/scripts/install_talib_linux.sh"]
RUN /app/scripts/install_talib_linux.sh
RUN pip install TA-Lib==0.4.24

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . /app

EXPOSE 9000
