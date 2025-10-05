# Redis RDB to JSON Converter

[![Join our Telegram RU](https://img.shields.io/badge/Telegram-RU-03A500?style=for-the-badge&logo=telegram&logoColor=white&labelColor=blue&color=red)](https://t.me/hidden_coding)
[![Join our Telegram ENG](https://img.shields.io/badge/Telegram-EN-03A500?style=for-the-badge&logo=telegram&logoColor=white&labelColor=blue&color=red)](https://t.me/hidden_coding_en)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/aero25x)
[![Twitter](https://img.shields.io/badge/Twitter-1DA1F2?style=for-the-badge&logo=x&logoColor=white)](https://x.com/aero25x)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@flaming_chameleon)
[![Reddit](https://img.shields.io/badge/Reddit-FF3A00?style=for-the-badge&logo=reddit&logoColor=white)](https://www.reddit.com/r/HiddenCode/)


A lightweight, **Python-based Redis RDB parser** that converts **Redis dump.rdb files** to **JSON format**. Perfect for **migrating Redis data**, **backup analysis**, or **debugging Redis databases**. Supports **RDB version 12 (Redis 7.x)** with full parsing of strings, lists, sets, sorted sets (ZSET), hashes, streams, and more. SEO-optimized for developers searching for "Redis RDB to JSON converter" or "parse Redis RDB file".

[![GitHub stars](https://img.shields.io/github/stars/aero25x/redis-rdb-convert?logo=github&style=flat-square)](https://github.com/aero25x/redis-rdb-convert/stargazers)
[![Python Version](https://img.shields.io/badge/python-3.7%2B-blue?logo=python&style=flat-square)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Issues](https://img.shields.io/github/issues/aero25x/redis-rdb-convert?color=red&style=flat-square)](https://github.com/aero25x/redis-rdb-convert/issues)

## 🚀 Quick Start

Transform your Redis RDB dump into searchable JSON in seconds:

```bash
# Without Redis
pip install -r requirements.txt  # Optional: for LZF decompression
python rdb_parser.py dump.rdb output.json --pretty --simple

# With Redis Server - make sure you have backup.rdb file
dokcer-compose up -d
python3 export_from_redis.py output.json
```

Output example (simplified JSON):
```json
{
  "keys": {
    "user:123": "John Doe",
    "cart:456": ["item1", "item2"],
    "scores": [{"member": "alice", "score": 95.5}]
  }
}
```

## 🌟 Key Features

- **Full RDB Parsing**: Handles **Redis RDB version 12** (Redis 7.x) including legacy formats like ziplists, quicklists, and listpacks.
- **Data Types Supported**:
  - Strings (with LZF compression support)
  - Lists (ziplist, quicklist)
  - Sets (intset, listpack)
  - Sorted Sets (ZSET, ziplist/listpack encoded)
  - Hashes (ziplist, listpack)
  - Streams (simplified parsing)
- **Metadata Extraction**: Captures expiry times, idle/frequency counters, DB selection, and AUX fields.
- **Flexible Output**: 
  - **Simple mode**: Raw values only.
  - **Full mode**: Includes type, expiry, and timestamps.
- **Error-Resilient**: Skips invalid entries, handles binary data gracefully, and logs warnings.
- **SEO-Friendly**: Optimized for searches like "convert Redis RDB to JSON", "Redis backup parser Python", "RDB dump analyzer".
- **Lightweight**: No heavy dependencies (optional `python-lzf` for compression).

## 📦 Installation

1. **Clone the Repo**:
   ```bash
   git clone https://github.com/aero25x/redis-rdb-convert.git
   cd redis-rdb-convert
   ```

2. **Install Dependencies** (Python 3.7+ required):
   ```bash
   pip install python-lzf  # Optional: For LZF decompression of strings
   ```

3. **Run the Parser**:
   ```bash
   python rdb_parser.py /path/to/dump.rdb [output.json] [--pretty] [--simple]
   ```

## 📖 Usage Examples

### Basic Conversion
Convert a full RDB dump to pretty-printed JSON:
```bash
python rdb_parser.py backup.rdb --pretty
```
*Output*: Prints JSON to stdout with full metadata (e.g., expiry dates in ISO format).

### Simple Values Only
Extract just key-value pairs (no metadata):
```bash
python rdb_parser.py dump.rdb output.json --simple
```

### Advanced: Parse Specific DB
The parser auto-detects DB changes via `SELECTDB` opcodes. For multi-DB dumps, all data is consolidated under the last DB (customize in code if needed).

### Programmatic Use
```python
from rdb_parser import RDBParser

parser = RDBParser('dump.rdb', simple_format=True)
result = parser.parse()
print(json.dumps(result['keys'], indent=2))
```

## 🔍 Supported RDB Structures

| RDB Type | Encoding | Description | Example Output |
|----------|----------|-------------|----------------|
| **String** | Raw/LZF/Int | Basic key-value | `"value"` |
| **List** | Ziplist/Quicklist | Ordered array | `["item1", "item2"]` |
| **Set** | Intset/Listpack | Unique members | `["a", "b"]` |
| **ZSET** | Ziplist/Listpack | Member-score pairs | `[{"member": "alice", "score": 95.5}]` |
| **Hash** | Ziplist/Listpack | Field-value map | `{"field1": "val1"}` |
| **Stream** | Listpacks | Entries (simplified) | `"<stream with N elements>"` |

*Note*: Binary data is hex-encoded; large strings (>100MB) are skipped for safety.

## ⚠️ Limitations & Known Issues

- **Compression**: LZF requires `python-lzf`; otherwise, compressed strings show as placeholders.
- **Streams**: Basic support—full consumer groups not parsed.
- **Very Large Files**: Memory-intensive for GB-scale RDBs; process in chunks if needed.
- **Older Versions**: Optimized for RDB v12; test with v5-v11 for compatibility.
- **No Modules**: Skips Redis Modules (TYPE_MODULE).

For issues like "RDB parse error" or "ziplist decoding failed", check stderr logs or open an issue.

## 🤝 Contributing

Love this **Redis RDB converter**? Help make it better!

1. Fork the repo.
2. Create a feature branch (`git checkout -b feature/amazing-feature`).
3. Commit changes (`git commit -m 'Add amazing feature'`).
4. Push to branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

**Guidelines**: Add tests for new encodings, update `parse_ziplist` for edge cases. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## 📄 License

This **Redis RDB to JSON parser** is MIT-licensed. See [LICENSE](LICENSE) for details.

## 📊 Related Tools

- [rdbtools](https://github.com/sripathikrishnan/redis-rdb-tools): C-based alternative.
- [redis-dump-go](https://github.com/nborwankar/redis-dump-go): Go implementation.
- Searching for "Redis RDB recovery" or "migrate Redis to JSON"? This tool fits perfectly!

---

**Keywords**: Redis RDB parser, RDB to JSON, Redis dump converter, parse Redis backup, Redis 7 RDB, ziplist decoder, listpack parser.

*Built with ❤️ for Redis devs. Star ⭐ if it saves your day!*


[![Join our Telegram RU](https://img.shields.io/badge/Telegram-RU-03A500?style=for-the-badge&logo=telegram&logoColor=white&labelColor=blue&color=red)](https://t.me/hidden_coding)
[![Join our Telegram ENG](https://img.shields.io/badge/Telegram-EN-03A500?style=for-the-badge&logo=telegram&logoColor=white&labelColor=blue&color=red)](https://t.me/hidden_coding_en)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/aero25x)
[![Twitter](https://img.shields.io/badge/Twitter-1DA1F2?style=for-the-badge&logo=x&logoColor=white)](https://x.com/aero25x)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@flaming_chameleon)
[![Reddit](https://img.shields.io/badge/Reddit-FF3A00?style=for-the-badge&logo=reddit&logoColor=white)](https://www.reddit.com/r/HiddenCode/)
