import os
import sys
from pathlib import Path

# Disable the background download coordinator during tests to keep the
# environment deterministic.
os.environ.setdefault("DISABLE_DOWNLOAD_COORDINATOR", "true")
os.environ.setdefault("USE_CF_BYPASS", "false")
os.environ.setdefault("AA_BASE_URL", "https://example.com")
os.environ.setdefault("HTTP_PROXY", "")
os.environ.setdefault("HTTPS_PROXY", "")

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
