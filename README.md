[README.md](https://github.com/user-attachments/files/34526347/README.md)
# Speed Testing

SpeedTesting aims to measure user's internet speed on browser based applications. It measures latency, download and upload throughput, jitter, and latency under load through a FastAPI server.

## Features

- Download and upload throughput measurements using parallel transfers
- Idle latency using WebSocket echo probes, with HTTP fallback
- Jitter and application-level probe loss estimates
- Latency during download/upload traffic and a bufferbloat grade
- Live throughput chart, test statistics, and connection-quality indicators
- JSON export and recent-test history stored in the browser
- Local operation or optional deployment on a remote server (such as AWS EC2)

## Requirements

- **Python 3.10+** 
- **pip** and Python's built-in `venv` module
- A modern browser with JavaScript enabled
- Network access between the browser and the machine running the server


The `uvicorn[standard]` extra installs supporting packages for the ASGI server, including WebSocket support. 
Install dependencies from the repository's existing `backend/requirements.txt` file.

## Project structure

```text
CS568-SpeedTesting/
├── backend/
│   ├── main.py              # FastAPI endpoints and static frontend hosting
│   └── requirements.txt     # Python dependencies
├── frontend/
│   ├── index.html           # Web interface
│   ├── style.css            # Styling
│   └── app.js               # Browser-side measurement logic
└── .gitignore
```

## Quick start (local)

### 1. Clone the repository

```bash
git clone https://github.com/fulyaturkmenoglu/CS568-SpeedTesting.git
cd CS568-SpeedTesting
```

Run the remaining commands from the **repository root** unless instructed otherwise.

### 2. Create and activate a virtual environment

**Windows — PowerShell:**

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell prevents script activation, you can use the environment's Python executable directly without activating it (see below).

**Windows — Command Prompt:**

```bat
py -m venv .venv
.venv\Scripts\activate.bat
```

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Python dependencies

From the **repository root** with the virtual environment activated:

```bash
python -m pip install -r backend/requirements.txt
```

If you did not activate the environment, use its Python executable explicitly:

**Windows — PowerShell:**

```powershell
.\.venv\Scripts\python.exe -m pip install -r backend/requirements.txt
```

**macOS / Linux:**

```bash
./.venv/bin/python -m pip install -r backend/requirements.txt
```

### 4. Start the application

From the **repository root** with the environment activated:

```bash
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

Open **http://127.0.0.1:8000/** to use the speed test. API documentation is available at **http://127.0.0.1:8000/docs**. The health endpoint is **http://127.0.0.1:8000/api/health** and should return `{"status":"ok"}`.

Press `Ctrl+C` in the terminal to stop the server. `--reload` is intended for local development, **not** production deployment.


## Notes

- The `.venv/` directory is ignored by Git. Each contributor must create their own local environment.
- The browser stores up to 10 recent test summaries locally and can export a detailed test result as JSON.
- A remote server measures the path **between the browser and that server**; a local server mainly measures local communication and is not a meaningful substitute for an internet-speed endpoint.
- Repeated speed tests can transfer substantial amounts of data; consider metered connections and cloud bandwidth charges.

 **If you want to test your internet speed without configure the project you can use the following link to visit our live application hosted on AWS EC2
 (http://13.60.47.198/)**


With best regards
Yiğit & Fulya
