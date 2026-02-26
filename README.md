# soldb - M1-Optimized In-Memory Key-Value Store

A high-performance in-memory key-value store optimized for Apple Silicon (M1/M2/M3 Macs). Inspired by Redis, designed for learning and performance experimentation.

## Features

- **Hardware Acceleration**: Uses ARM CRC32C and NEON SIMD instructions on M1 Macs
- **Bloom Filter**: Fast negative lookups to avoid expensive hash collisions
- **Thread-Safe**: Lock-free atomic operations for concurrent access
- **Dynamic Resizing**: Automatic hash table expansion when load factor exceeds 0.75
- **Terminal UI**: Real-time visualization of operations using ncurses

## Building

```bash
# Requires ncurses
# On macOS
brew install ncurses

# Compile
g++ -O3 -o soldb soldb_m1.cpp -lncurses -lm

# Run (requires a CSV file with key,value pairs)
./soldb data.csv
```

## Architecture

- **Hash Table**: Open addressing with linear probing (128 max probes)
- **Entry Size**: 128 bytes (cache-line aligned)
- **Max Key Size**: 24 bytes
- **Max Value Size**: 24 bytes
- **Initial Table Size**: 65,536 slots
- **Bloom Filter**: 16K bits

## Performance

The database leverages M1 hardware features:
- `__crc32c*` intrinsics for fast hashing
- NEON SIMD for string comparisons
- Cache-line aligned entries to prevent false sharing

## Limitations

- Single-threaded command processing (multi-threaded internal operations)
- No network protocol (no RESP/server mode)
- No persistence (no disk storage)
- No TTL/expiration support
- Keys and values limited to 24 bytes
- Supports only string data type

## Operations

| Command | Description |
|---------|-------------|
| SET | Store a key-value pair |
| GET | Retrieve value by key |
| EXISTS | Check if key exists |
| DELETE | Remove key from database |

## License

MIT
