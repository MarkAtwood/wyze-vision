# Base on alexxit/go2rtc: it already bundles the static go2rtc 1.9.14 binary
# (KVS WebRTC capable), ffmpeg (needed for frame.jpeg H264->JPEG), and python3.
FROM alexxit/go2rtc:latest

# wyzeapy + its C-extension deps (aiodns/pycares/aiohttp/pycryptodome), Pillow
# (used to render the "offline since" placeholder), paho-mqtt (optional
# conn-state bridge, Hassio-708) and websockets (optional event-driven grab,
# Hassio-i0w) install from musllinux wheels with no build toolchain.
# --break-system-packages because the base python is PEP668 externally-managed;
# this is a single-purpose image.
RUN pip install --no-cache-dir --break-system-packages wyzeapy requests Pillow paho-mqtt websockets

WORKDIR /app
COPY snapshot.py /app/snapshot.py

ENV PYTHONUNBUFFERED=1

# Keep tini (the base entrypoint) as PID 1 so the go2rtc child is reaped.
ENTRYPOINT ["/sbin/tini", "--", "python3", "/app/snapshot.py"]
