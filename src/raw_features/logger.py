import os
from datetime import datetime

def get_required_env_var(name: str) -> str:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    raise RuntimeError(f"Missing required env var {name}")
  return value.strip()

BASE_DIR = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_ROOT")
LOGS_DIR = os.path.normpath(os.path.join(BASE_DIR, "logs"))
LOG_PATH = os.path.join(LOGS_DIR, "raw_features_spark_publisher.log")
_LOG_INITIALIZED = False


def _initialize_log() -> None:
  global _LOG_INITIALIZED
  if _LOG_INITIALIZED:
    return
  os.makedirs(LOGS_DIR, exist_ok=True)
  with open(LOG_PATH, "w", encoding="utf-8"):
    pass
  _LOG_INITIALIZED = True


def log_message(message: str, level: str = "INFO") -> None:
  _initialize_log()
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  line = f"{timestamp} -- RawFeaturesSparkPublisher -- {level} -- {message}\n"
  with open(LOG_PATH, "a", encoding="utf-8") as f:
    f.write(line)
