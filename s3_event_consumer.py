import json
import os
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

CONFIG_FILE = "s3_consumer_config.json"

# ANSI Color Codes for Fallback Formatting
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.", file=sys.stderr)
        sys.exit(1)
        
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def format_json_color(payload_str):
    """Formats JSON string with jq-like syntax highlighting."""
    try:
        # Try using Pygments for native jq-style terminal coloring
        from pygments import highlight
        from pygments.lexers import JsonLexer
        from pygments.formatters import TerminalFormatter

        payload = json.loads(payload_str)
        formatted = json.dumps(payload, indent=2)
        return highlight(formatted, JsonLexer(), TerminalFormatter()).strip()
    except ImportError:
        # Simple native ANSI coloring fallback if pygments is not installed
        try:
            payload = json.loads(payload_str)
            formatted = json.dumps(payload, indent=2)
            # Highlight keys in cyan and strings in green
            lines = []
            for line in formatted.splitlines():
                if ":" in line:
                    key, val = line.split(":", 1)
                    lines.append(f"{CYAN}{key}{RESET}:{GREEN}{val}{RESET}")
                else:
                    lines.append(f"{YELLOW}{line}{RESET}")
            return "\n".join(lines)
        except json.JSONDecodeError:
            return payload_str

def main():
    config = load_config(CONFIG_FILE)
    
    kafka_conf = config.get("kafka_config", {})
    topic = config.get("topic")

    if not topic:
        print("Error: 'topic' field missing in JSON configuration.", file=sys.stderr)
        sys.exit(1)

    consumer = Consumer(kafka_conf)
    consumer.subscribe([topic])

    print(f"Consumer started for topic '{topic}'. Waiting for messages...\n", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}", file=sys.stderr)
                    continue

            # Parse and print colorized output
            raw_data = msg.value().decode('utf-8')
            colored_output = format_json_color(raw_data)
            print(f"Received message:\n{colored_output}\n", flush=True)

    except KeyboardInterrupt:
        print("\nAborted by user.")

    finally:
        consumer.close()

if __name__ == '__main__':
    main()
