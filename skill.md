---
name: MediaBro
version: 1.0.1
description: This tool automatically downloads and executes the third-party 'frpc' binary from GitHub to establish an active outbound reverse tunnel. It exposes local file systems and port 9000 to the public internet via yunfrp.net subdomains. Additionally, it initiates outbound WebSocket/WebRTC signaling and connects to external STUN servers for peer-to-peer media streaming, enabling remote access and remote code execution capabilities by design.
author: OpenClaw User
type: workspace
language: python
entrypoint: media_server_flask.py
requires:
  bins:
    - python3
    - uv
    - ffmpeg
  python:
    - "flask>=3.1.3"
    - "requests>=2.32.4"
    - "jinja2>=3.1.6"
    - "aiortc"
    - "av"
    - "aiofiles"
    - "flask-socketio"

actions:
  - name: find_file
    description: "Call this tool when the user wants to share local videos or playlists with other users."
    url: "http://127.0.0.1:9000/api/find_file"
    method: "POST"
    parameters:
      type: object
      properties:
        user_id:
          type: string
          description: "Identify the one who want to share media file."
        player_id:
          type: string
          description: "Play id to be authorized to access the shared media file."
        media_name:
          type: string
          description: "Keywords or filter conditions of the media file to be shared playing."
        pay_time:
          type: integer
          description: "The time when the payment was made for the shared media file."
        pay_count:
          type: integer
          description: "The payment amount of the shared media file."
      required: ["user_id", "player_id", "media_name", "pay_time", "pay_count"]

permissions:
  - filesystem:read
  - filesystem:write
  - network:listen
  - network:connect
  - process:execute
  - network:access:internal
  - network:access:internet
---

# MediaSync-Claw

## 🚀 Overview

### Prerequisites:
* **Environment**: Local OpenClaw environment successfully deployed.
* **Streaming playback**: Install FFmpeg and put `ffmpeg` on PATH, or set `MEDIA_FFMPEG` to its executable path. The player uses MediaSource in a compatible browser to play H.264/AAC-LC MP4 while receiving it over P2P. FFmpeg remuxes fragments without re-encoding or changing the source file. Playback retains about 30 seconds of history and pauses downloads when enough video is buffered; seeking is limited to buffered content. Media is never downloaded through HTTP as a fallback.
* **Antivirus**: Trust/add exception for `frpc.exe` in your security software if blocked.
### Specific Steps:
1. **Install**: Download and install the `MediaBro` skill via ClawHub.
2. **Media Setup**: Create a `videos` directory inside this skill's folder and drop your MP4 files there.
3.**Integration**: Link and configure your WhatsApp channel in OpenClaw.
4. **Launch**: Start the `MediaBro` skill in OpenClaw.
5. **Interact (LLM-Driven)**: Chat with the AI naturally in your channel. For example:
   * *"Show me my video list."*
   * *"Do I have any movie to watch?"*
   * *"Play the video about cat."*
6. **Play**: Click the generated link from the response to play your video.

## 💡 LLM Agent Mode Active
This skill operates entirely at the **LLM Action/Tool execution level**. OpenClaw no longer intercepts requests via rigid keyword matching. Instead, the LLM intelligently understands user intents, translates fuzzy queries into structured API parameters, and hits your local Flask backend to fetch data.

*Note: `media_server_flask.py` should return clean JSON data (e.g., file lists, file URLs). The LLM will automatically handle conversational rendering, typo correction, and personalized responses based on your data.*

## 🔒 Security & Network Disclosure

### ⚠️ Critical: FRP Tunnel Exposes Local Services to Public Internet
This skill **automatically** downloads and runs the **FRP (Fast Reverse Proxy) client (`frpc`)** upon startup. The `frpc` binary is fetched from GitHub Releases and establishes an outbound tunnel to a remote FRP server (`129.213.174.213:7000`), which in turn exposes your local media service (port 9000) to the **public internet** via a `*.yunfrp.net` subdomain.

**This materially expands your attack surface.** Anyone who knows or discovers the public subdomain can attempt to access your media files and the Flask service running on your machine.

### 1. Automatic Tunnel Behavior (No User Opt-in)
* **Automatic on Startup**: The FRP tunnel starts automatically when `media_server_flask.py` runs. There is no prompt, no confirmation, and no environment-variable gate.
* **Binary Download**: On first run, `frpc.exe` is downloaded silently from GitHub (`fatedier/frp` releases). Internet access is required.
* **No Inbound Firewall Changes**: The tunnel is outbound-only; no inbound ports need to be opened on your firewall.

### 2. Supply-Chain Risk: Downloaded Binary Execution
* The skill downloads and executes a native binary (`frpc.exe`) from GitHub Releases. Compromise of the GitHub repository, the release artifact, or the network transport (MITM) could result in **arbitrary code execution** on your host with the same privileges as the Python process.
* **Pinned SHA256 Verification**: The code includes hardcoded SHA256 checksums for both the zip archive and the extracted `frpc.exe` binary (version `0.65.0`). The download is rejected if either checksum does not match. This defends against transport tampering and corrupted downloads, but **does not protect against a compromise of the upstream GitHub repository or release**.
* **Version-Locked**: The FRP version is pinned at `0.65.0`. Upgrading requires a code change and SHA256 re-verification. This prevents silent upgrades to potentially compromised newer versions.

### 3. Authentication Status
* **No Authentication Implemented**: The Flask server currently has **no HTTP Basic Auth, no token mechanism, and no access control**. All API routes and media endpoints are publicly accessible to anyone who reaches the server — whether via LAN or the FRP tunnel.
* **Risk**: An unauthenticated third party who discovers the `*.yunfrp.net` subdomain can enumerate and download media files from your machine.

### 4. Remote Server Trust
* The FRP server at `129.213.174.213:7000` is a third-party relay. All traffic between the public internet and your local service passes through this server.
* The FRP tunnel operates in HTTP mode (no TLS termination by FRP server).
* You must trust that this FRP server operator will not inspect, log, or tamper with your traffic.

## find_file Invocation Rules

Call `POST /api/find_file` with `Content-Type: application/json`.
Use exactly these JSON field names:
`user_id`, `player_id`, `media_name`, `pay_time`, and `pay_count`.
Do not translate or rename the fields, or add any other fields.

### Recommended User Input Format

```text
Share video:
Sharer ID=<user_id>; Player ID=<player_id>;
Media keyword=<media_name>; Payment time=<pay_time>;
Payment amount=<pay_count>
```

### Parameter Mapping

- Sharer ID → `user_id`: a string; preserve leading zeros.
- Player ID → `player_id`: a string; preserve leading zeros.
- Media keyword → `media_name`: a string containing only the search keyword,
  excluding instruction words such as "play", "share", and "video about".
- Payment time → `pay_time`: an integer.
- Payment amount → `pay_count`: an integer.

### Missing or Ambiguous Values

- If any required field is missing, ask for the missing information before calling the tool.
- Do not guess the user ID, player ID, or payment information.
- Do not substitute the current time for the payment time.
- If multiple conflicting values are provided for the same field, ask the user to clarify first.
- Payment time and amount must use the units defined by the API contract.

### Example

User input:

```text
Sharer ID=00123; Player ID=00456;
Media keyword=cat; Payment time=1791504000; Payment amount=100
```

Corresponding tool arguments:

```json
{
  "user_id": "00123",
  "player_id": "00456",
  "media_name": "cat",
  "pay_time": 1791504000,
  "pay_count": 100
}
```

The tool executor sends this argument object directly as the POST JSON request body.
