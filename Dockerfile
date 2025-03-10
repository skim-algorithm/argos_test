# Step 1: Python 공식 이미지 사용
FROM python:3.13-slim

# Step 2: 필수 패키지 설치 (TA-Lib 빌드에 필요한 의존성 포함)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    gcc \
    git \
    make \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Step 3: 최신 TA-Lib 다운로드 및 빌드 (CMake 사용)
RUN git clone https://github.com/TA-Lib/ta-lib.git && \
    cd ta-lib && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && \
    make -j$(nproc) && \
    make install && \
    cd ../.. && rm -rf ta-lib

# Step 4: Python 패키지 설치 (TA-Lib 최신 버전 포함)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install TA-Lib

COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9000
