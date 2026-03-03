RAW_FEATURES_SPARK_PUBLISHER_ROOT ?= $(CURDIR)
RAW_FEATURES_SPARK_PUBLISHER_ASSETS ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets/filings_10k
RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets_export.zip
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST ?= localhost
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT ?= 9092
RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL ?= raw_features
PYTHON ?= /home/linuxu/anaconda3/bin/python
company ?=
assets_dir := $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)
publisher_script := $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/raw_features_spark_publisher.py
supported_component := raw_features
selected_component := $(firstword $(MAKECMDGOALS))

.DEFAULT_GOAL := help

.PHONY: help raw_features process do_export do_import

help:
	@echo "Usage: make raw_features <action> [options]"
	@echo ""
	@echo "Component:"
	@echo "  raw_features            Supported component."
	@echo ""
	@echo "Actions (require component target first):"
	@echo "  process                 Run raw_features_spark_publisher.py."
	@echo "                          Optional: company=<ticker> (single-company mode)."
	@echo "  do_export               Zip assets dir to assets export archive."
	@echo "  do_import               Import assets archive unless assets dir is non-empty."
	@echo ""
	@echo "Options (override with make VAR=value):"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ROOT                  default: $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS                default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT         default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL  default: $(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)"
	@echo "  PYTHON                                             default: $(PYTHON)"
	@echo ""
	@echo "Examples:"
	@echo "  make raw_features process"
	@echo "  make raw_features process company=aaoi"
	@echo "  make raw_features do_export"
	@echo "  make raw_features do_import"

raw_features:
	@if [ "$(selected_component)" != "$(supported_component)" ]; then \
		echo "ERROR: unsupported component '$(selected_component)'. Supported component: $(supported_component)"; \
		exit 1; \
	fi
	@if [ "$(words $(MAKECMDGOALS))" -eq 1 ]; then \
		echo "INFO: component '$(supported_component)' selected. Choose one action: process | do_export | do_import"; \
	fi

process: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(publisher_script)" ]; then \
		echo "ERROR: publisher script not found: $(publisher_script)"; \
		exit 1; \
	fi
	@if [ -n "$(company)" ]; then \
		echo "INFO: process mode single company=$(company)"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_COMPANY="$(company)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	else \
		echo "INFO: process mode full assets scan"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	fi

do_export: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -d "$(assets_dir)" ]; then \
		echo "ERROR: assets dir not found: $(assets_dir)"; \
		exit 1; \
	fi
	@mkdir -p "$(dir $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT))"
	@rm -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@cd "$(assets_dir)" && zip -rq "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" .
	@echo "INFO: assets exported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"

do_import: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" ]; then \
		echo "ERROR: export zip not found: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"; \
		exit 1; \
	fi
	@if [ -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" ] && [ "$$(ls -A "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" 2>/dev/null)" ]; then \
		echo "WARN: assets import is skipped because assets already exist: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
	else \
		mkdir -p "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
			unzip -qo "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
			echo "INFO: assets imported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
		fi

%:
	@echo "ERROR: unsupported component '$@'. Supported component: $(supported_component)"
	@exit 1
