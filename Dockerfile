# Base on alexxit/go2rtc: it already bundles the static go2rtc 1.9.14 binary
# (KVS WebRTC capable), ffmpeg (needed for frame.jpeg H264->JPEG), and python3.
FROM alexxit/go2rtc:latest

# wyzeapy + its C-extension deps (aiodns/pycares/aiohttp/pycryptodome), Pillow
# (used to render the "offline since" placeholder), paho-mqtt (optional
# conn-state bridge) and websockets (optional event-driven grab) install from
# musllinux wheels with no build toolchain. Versions are pinned in
# requirements.txt (matching the deployed image).
# --break-system-packages because the base python is PEP668 externally-managed;
# this is a single-purpose image.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --break-system-packages -r /app/requirements.txt

WORKDIR /app
COPY snapshot.py /app/snapshot.py

ENV PYTHONUNBUFFERED=1

# Keep tini (the base entrypoint) as PID 1 so the go2rtc child is reaped.
ENTRYPOINT ["/sbin/tini", "--", "python3", "/app/snapshot.py"]
