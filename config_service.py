import configparser
import os


class ConfigError(Exception):
    """
    Raised when configuration is invalid or incomplete.
    Carries optional section/key context for better diagnostics.
    """
    def __init__(self, message, section=None, key=None):
        self.section = section
        self.key = key

        full_message = message
        if section:
            full_message += f" | section={section}"
        if key:
            full_message += f" | key={key}"

        super().__init__(full_message)


class Config:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise ConfigError("Config file not found", key=path)

        self._parser = configparser.ConfigParser()
        self._parser.read(path)

        self._load_pipeline()
        self._load_input()
        self._load_output()
        self._load_memory()
        self._load_anomaly()

    # -------------------------
    # Section loaders
    # -------------------------
    def _load_pipeline(self):
        section = "PIPELINE"
        self._require(section, ["chunk_size", "max_rows", "enable_checkpoint", "checkpoint_file"])

        self.chunk_size = self._get_int(section, "chunk_size")
        self.max_rows = self._get_int(section, "max_rows")
        self.enable_checkpoint = self._get_bool(section, "enable_checkpoint")
        self.checkpoint_file = self._get_str(section, "checkpoint_file")

    def _load_input(self):
        section = "INPUT"
        self._require(section, ["input_type", "input_path"])

        self.input_type = self._get_str(section, "input_type").lower()
        self.input_path = self._get_str(section, "input_path")
        self.file_pattern = self._get_str(section, "file_pattern", default=None)

        if self.input_type not in ("file", "directory"):
            raise ConfigError(
                "input_type must be 'file' or 'directory'",
                section=section,
                key="input_type"
            )

        if self.input_type == "directory" and not self.file_pattern:
            raise ConfigError(
                "file_pattern required when input_type=directory",
                section=section,
                key="file_pattern"
            )

    def _load_output(self):
        section = "OUTPUT"
        self._require(section, ["output_dir", "format"])

        self.output_dir = self._get_str(section, "output_dir")
        self.output_format = self._get_str(section, "format").lower()

        if self.output_format not in ("csv",):
            raise ConfigError(
                "Only CSV output is supported",
                section=section,
                key="format"
            )

        os.makedirs(self.output_dir, exist_ok=True)

    def _load_memory(self):
        section = "MEMORY"
        self._require(section, ["max_chunk_mb", "flush_interval"])

        self.max_chunk_mb = self._get_int(section, "max_chunk_mb")
        self.flush_interval = self._get_int(section, "flush_interval")

    def _load_anomaly(self):
        section = "ANOMALY"
        self._require(section, ["top_n", "high_revenue_threshold"])

        self.anomaly_top_n = self._get_int(section, "top_n")
        self.high_revenue_threshold = self._get_float(section, "high_revenue_threshold")

    # -------------------------
    # Helpers
    # -------------------------
    def _require(self, section, keys):
        if not self._parser.has_section(section):
            raise ConfigError("Missing required section", section=section)

        for key in keys:
            if not self._parser.has_option(section, key):
                raise ConfigError(
                    "Missing required config key",
                    section=section,
                    key=key
                )

    def _get_str(self, section, key, default=None):
        return self._parser.get(section, key, fallback=default)

    def _get_int(self, section, key):
        try:
            return self._parser.getint(section, key)
        except ValueError:
            raise ConfigError(
                "Invalid integer value",
                section=section,
                key=key
            )

    def _get_float(self, section, key):
        try:
            return self._parser.getfloat(section, key)
        except ValueError:
            raise ConfigError(
                "Invalid float value",
                section=section,
                key=key
            )

    def _get_bool(self, section, key):
        try:
            return self._parser.getboolean(section, key)
        except ValueError:
            raise ConfigError(
                "Invalid boolean value",
                section=section,
                key=key
            )
