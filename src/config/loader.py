import yaml
from .paths import CONFIG_DIR

def load_settings() -> dict:
    settings_path = CONFIG_DIR / "settings.yaml"
    with open(settings_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
