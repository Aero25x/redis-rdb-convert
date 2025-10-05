#!/usr/bin/env python3
"""
RDB (Redis Database) file parser that converts to JSON.
Supports RDB version 12 (Redis 7.x)
"""

import struct
import json
import sys
import zlib
from datetime import datetime

class RDBParser:
    # RDB opcodes
    OPCODE_IDLE = 0xF8
    OPCODE_FREQ = 0xF9
    OPCODE_AUX = 0xFA
    OPCODE_RESIZEDB = 0xFB
    OPCODE_EXPIRETIME_MS = 0xFC
    OPCODE_EXPIRETIME = 0xFD
    OPCODE_SELECTDB = 0xFE
    OPCODE_EOF = 0xFF

    # Value types
    TYPE_STRING = 0
    TYPE_LIST = 1
    TYPE_SET = 2
    TYPE_ZSET = 3
    TYPE_HASH = 4
    TYPE_ZSET_2 = 5
    TYPE_MODULE = 6
    TYPE_MODULE_2 = 7
    TYPE_HASH_ZIPMAP = 9
    TYPE_LIST_ZIPLIST = 10
    TYPE_SET_INTSET = 11
    TYPE_ZSET_ZIPLIST = 12
    TYPE_HASH_ZIPLIST = 13
    TYPE_LIST_QUICKLIST = 14
    TYPE_STREAM_LISTPACKS = 15
    TYPE_HASH_LISTPACK = 16
    TYPE_ZSET_LISTPACK = 17
    TYPE_LIST_QUICKLIST_2 = 18
    TYPE_STREAM_LISTPACKS_2 = 19
    TYPE_SET_LISTPACK = 20
    TYPE_STREAM_LISTPACKS_3 = 21

    # Length encoding constants
    RDB_6BITLEN = 0
    RDB_14BITLEN = 1
    RDB_32BITLEN = 0x80
    RDB_64BITLEN = 0x81
    RDB_ENCVAL = 3

    # String encoding
    RDB_ENC_INT8 = 0
    RDB_ENC_INT16 = 1
    RDB_ENC_INT32 = 2
    RDB_ENC_LZF = 3

    def __init__(self, filename, simple_format=False):
        self.filename = filename
        self.data = {}
        self.aux_data = {}
        self.current_db = 0
        self.file = None
        self.simple_format = simple_format

    def read_byte(self):
        byte = self.file.read(1)
        if not byte:
            raise EOFError("Unexpected end of file")
        return byte[0]

    def read_bytes(self, n):
        data = self.file.read(n)
        if len(data) != n:
            raise EOFError(f"Expected {n} bytes, got {len(data)}")
        return data

    def read_signed_byte(self):
        return struct.unpack('b', self.read_bytes(1))[0]

    def read_signed_short(self):
        return struct.unpack('<h', self.read_bytes(2))[0]

    def read_signed_int(self):
        return struct.unpack('<i', self.read_bytes(4))[0]

    def read_unsigned_int(self):
        return struct.unpack('<I', self.read_bytes(4))[0]

    def read_unsigned_long(self):
        return struct.unpack('<Q', self.read_bytes(8))[0]

    def read_length_with_encoding(self):
        """Read length encoding and return (length, is_encoded, encoding_type)"""
        byte = self.read_byte()

        enc_type = (byte & 0xC0) >> 6

        if enc_type == self.RDB_ENCVAL:
            # Special encoding (11xxxxxx)
            encoding = byte & 0x3F
            return None, True, encoding
        elif enc_type == self.RDB_6BITLEN:
            # 6-bit length (00xxxxxx)
            return byte & 0x3F, False, None
        elif enc_type == self.RDB_14BITLEN:
            # 14-bit length (01xxxxxx)
            next_byte = self.read_byte()
            return ((byte & 0x3F) << 8) | next_byte, False, None
        elif enc_type == 2:
            # This means top 2 bits are 10
            # Check if it's 32-bit (10000000) or 64-bit (10000001)
            remaining = byte & 0x3F
            if remaining == 0:
                # 32-bit length
                return self.read_unsigned_int(), False, None
            elif remaining == 1:
                # 64-bit length
                return self.read_unsigned_long(), False, None
            else:
                # Reserved for future use, try to read as 32-bit
                return self.read_unsigned_int(), False, None
        else:
            raise ValueError(f"Unknown length encoding: {byte}")

    def read_length(self):
        """Read length encoding"""
        length, is_encoded, _ = self.read_length_with_encoding()
        if is_encoded:
            raise ValueError("Unexpected encoded value in length")
        return length

    def read_string(self):
        """Read string encoding"""
        length, is_encoded, encoding = self.read_length_with_encoding()

        if is_encoded:
            if encoding == self.RDB_ENC_INT8:
                return str(self.read_signed_byte())
            elif encoding == self.RDB_ENC_INT16:
                return str(self.read_signed_short())
            elif encoding == self.RDB_ENC_INT32:
                return str(self.read_signed_int())
            elif encoding == self.RDB_ENC_LZF:
                # LZF compressed string
                compressed_len = self.read_length()
                uncompressed_len = self.read_length()
                compressed_data = self.read_bytes(compressed_len)
                try:
                    # Try to decompress (LZF is not standard, but try common approaches)
                    uncompressed = self.lzf_decompress(compressed_data, uncompressed_len)
                    return uncompressed.decode('utf-8', errors='replace')
                except:
                    return f"<compressed:{compressed_len} bytes>"
            else:
                # This might not be a special encoding, it might be a regular length
                # Encoding values > 3 in special encoding shouldn't happen
                # This might be a parsing error - treat as length
                print(f"Warning: Unexpected encoding value {encoding}, treating as regular length", file=sys.stderr)
                # Re-interpret: we already consumed the length byte, so we can't go back
                # Return placeholder and hope the stream recovers
                return f"<parse_error_enc:{encoding}>"
        else:
            # Regular string
            if length == 0:
                return ""
            if length < 0 or length > 1024 * 1024 * 100:  # Max 100MB strings
                print(f"Warning: Suspicious string length {length}, skipping", file=sys.stderr)
                return f"<invalid_length:{length}>"
            data = self.read_bytes(length)
            try:
                return data.decode('utf-8', errors='replace')
            except:
                # If decode fails, return hex
                try:
                    return data.hex()
                except:
                    return "<binary_data>"

    def read_string_raw(self):
        """Read string as raw bytes (for binary structures like listpack/ziplist)"""
        length, is_encoded, encoding = self.read_length_with_encoding()

        if is_encoded:
            if encoding == self.RDB_ENC_INT8:
                # Return single byte as bytes
                return bytes([self.read_signed_byte() & 0xFF])
            elif encoding == self.RDB_ENC_INT16:
                return struct.pack('<h', self.read_signed_short())
            elif encoding == self.RDB_ENC_INT32:
                return struct.pack('<i', self.read_signed_int())
            elif encoding == self.RDB_ENC_LZF:
                # LZF compressed string
                compressed_len = self.read_length()
                uncompressed_len = self.read_length()
                compressed_data = self.read_bytes(compressed_len)
                try:
                    return self.lzf_decompress(compressed_data, uncompressed_len)
                except:
                    return compressed_data
            else:
                print(f"Warning: Unknown encoding in raw read: {encoding}", file=sys.stderr)
                return b""
        else:
            # Regular string - return raw bytes
            if length == 0:
                return b""
            return self.read_bytes(length)

    def lzf_decompress(self, data, expected_length):
        """Simple LZF decompression attempt"""
        # LZF is not in standard library, so we'll try basic approach
        # For production, use python-lzf library
        try:
            import lzf
            return lzf.decompress(data, expected_length)
        except ImportError:
            # If lzf not available, return placeholder
            return b"<LZF compressed data - install python-lzf to decompress>"

    def read_double(self):
        """Read double value"""
        length = self.read_byte()
        if length == 255:
            return float('-inf')
        elif length == 254:
            return float('inf')
        elif length == 253:
            return float('nan')
        else:
            data = self.read_bytes(length)
            return float(data.decode('ascii'))

    def read_list_ziplist(self):
        """Read ziplist encoded list"""
        ziplist_bytes = self.read_string_raw()
        try:
            return self.parse_ziplist(ziplist_bytes)
        except Exception as e:
            print(f"Failed to parse ziplist: {e}", file=sys.stderr)
            return []

    def read_set_intset(self):
        """Read intset encoded set"""
        intset_bytes = self.read_string_raw()
        # Simplified intset parsing
        if len(intset_bytes) < 8:
            return []
        try:
            encoding = struct.unpack('<I', intset_bytes[0:4])[0]
            length = struct.unpack('<I', intset_bytes[4:8])[0]
            result = []
            pos = 8
            size = [2, 4, 8][encoding] if encoding < 3 else 4
            for _ in range(min(length, 1000)):  # Limit to prevent issues
                if pos + size > len(intset_bytes):
                    break
                if encoding == 2:
                    val = struct.unpack('<h', intset_bytes[pos:pos+2])[0]
                elif encoding == 4:
                    val = struct.unpack('<i', intset_bytes[pos:pos+4])[0]
                elif encoding == 8:
                    val = struct.unpack('<q', intset_bytes[pos:pos+8])[0]
                else:
                    break
                result.append(str(val))
                pos += size
            return result
        except:
            return []

    def read_zset_ziplist(self):
        """Read ziplist encoded sorted set"""
        ziplist_bytes = self.read_string_raw()
        try:
            entries = self.parse_ziplist(ziplist_bytes)
            # Convert to zset (member-score pairs)
            zset_data = []
            for i in range(0, len(entries), 2):
                if i + 1 < len(entries):
                    member = str(entries[i])
                    score = float(entries[i + 1]) if isinstance(entries[i + 1], (int, float)) else 0
                    zset_data.append({"member": member, "score": score})
            return zset_data
        except Exception as e:
            print(f"Failed to parse zset ziplist: {e}", file=sys.stderr)
            return []

    def read_hash_ziplist(self):
        """Read ziplist encoded hash"""
        ziplist_bytes = self.read_string_raw()
        try:
            entries = self.parse_ziplist(ziplist_bytes)
            # Convert to hash (key-value pairs)
            hash_data = {}
            for i in range(0, len(entries), 2):
                if i + 1 < len(entries):
                    key = str(entries[i])
                    value = str(entries[i + 1])
                    hash_data[key] = value
            return hash_data
        except Exception as e:
            print(f"Failed to parse hash ziplist: {e}", file=sys.stderr)
            return {}

    def read_hash_listpack(self):
        """Read listpack encoded hash"""
        entries = self.read_listpack()
        if isinstance(entries, str):
            return {entries: ""}

        # Convert list to hash (key-value pairs)
        hash_data = {}
        for i in range(0, len(entries), 2):
            if i + 1 < len(entries):
                key = str(entries[i])
                value = str(entries[i + 1])
                hash_data[key] = value
        return hash_data

    def read_zset_listpack(self):
        """Read listpack encoded sorted set"""
        entries = self.read_listpack()
        if isinstance(entries, str):
            return [{"member": entries, "score": 0}]

        # Convert to zset (member-score pairs)
        zset_data = []
        for i in range(0, len(entries), 2):
            if i + 1 < len(entries):
                member = str(entries[i])
                score = float(entries[i + 1]) if isinstance(entries[i + 1], (int, float)) else 0
                zset_data.append({"member": member, "score": score})
        return zset_data

    def read_set_listpack(self):
        """Read listpack encoded set"""
        entries = self.read_listpack()
        if isinstance(entries, str):
            return [entries]
        return [str(e) for e in entries]

    def read_quicklist(self):
        """Read quicklist (Redis 3.2+)"""
        size = self.read_length()
        result = []
        for i in range(size):
            # Each entry is a ziplist - use read_string_raw to get raw bytes
            ziplist_bytes = self.read_string_raw()

            # Try to parse ziplist
            try:
                entries = self.parse_ziplist(ziplist_bytes)
                result.extend(entries)
            except Exception as e:
                print(f"Failed to parse ziplist {i} in quicklist: {e}", file=sys.stderr)
                continue

        return result

    def parse_ziplist(self, data):
        """Parse ziplist format"""
        if len(data) < 10:
            return []

        # Ziplist header
        # zlbytes (4), zltail (4), zllen (2)
        zlbytes = struct.unpack('<I', data[0:4])[0]
        zltail = struct.unpack('<I', data[4:8])[0]
        zllen = struct.unpack('<H', data[8:10])[0]

        result = []
        pos = 10

        # If zllen is less than 65535, it's the actual count
        # Otherwise we need to traverse
        count = zllen if zllen < 65535 else 999999

        for i in range(count):
            if pos >= len(data) - 1:
                break

            # Check for end marker
            if data[pos] == 0xFF:
                break

            try:
                value, bytes_read = self.parse_ziplist_entry(data[pos:])
                if value is not None:
                    result.append(value)
                pos += bytes_read
                if bytes_read == 0:
                    break
            except Exception as e:
                print(f"Error parsing ziplist entry {i}: {e}", file=sys.stderr)
                break

        return result

    def parse_ziplist_entry(self, data):
        """Parse a single ziplist entry"""
        if len(data) < 2:
            return None, 0

        # Previous entry length (1 or 5 bytes)
        prevlen = data[0]
        offset = 1

        if prevlen == 0xFE:
            if len(data) < 5:
                return None, 0
            prevlen = struct.unpack('<I', data[1:5])[0]
            offset = 5

        if offset >= len(data):
            return None, 0

        # Encoding byte
        encoding = data[offset]
        offset += 1

        # String encodings (00, 01, 10 prefix)
        if (encoding & 0xC0) == 0x00:
            # 6-bit length
            length = encoding & 0x3F
            if offset + length > len(data):
                return None, 0
            value = data[offset:offset + length].decode('utf-8', errors='replace')
            return value, offset + length

        elif (encoding & 0xC0) == 0x40:
            # 14-bit length
            if offset >= len(data):
                return None, 0
            length = ((encoding & 0x3F) << 8) | data[offset]
            offset += 1
            if offset + length > len(data):
                return None, 0
            value = data[offset:offset + length].decode('utf-8', errors='replace')
            return value, offset + length

        elif (encoding & 0xC0) == 0x80:
            # 32-bit length
            if offset + 4 > len(data):
                return None, 0
            length = struct.unpack('<I', data[offset:offset + 4])[0]
            offset += 4
            if offset + length > len(data):
                return None, 0
            value = data[offset:offset + length].decode('utf-8', errors='replace')
            return value, offset + length

        # Integer encodings
        elif encoding == 0xC0:
            # 16-bit integer
            if offset + 2 > len(data):
                return None, 0
            value = struct.unpack('<h', data[offset:offset + 2])[0]
            return value, offset + 2

        elif encoding == 0xD0:
            # 32-bit integer
            if offset + 4 > len(data):
                return None, 0
            value = struct.unpack('<i', data[offset:offset + 4])[0]
            return value, offset + 4

        elif encoding == 0xE0:
            # 64-bit integer
            if offset + 8 > len(data):
                return None, 0
            value = struct.unpack('<q', data[offset:offset + 8])[0]
            return value, offset + 8

        elif encoding == 0xF0:
            # 24-bit integer
            if offset + 3 > len(data):
                return None, 0
            value_bytes = data[offset:offset + 3]
            value = int.from_bytes(value_bytes, byteorder='little', signed=True)
            return value, offset + 3

        elif encoding == 0xFE:
            # 8-bit integer
            if offset + 1 > len(data):
                return None, 0
            value = struct.unpack('b', data[offset:offset + 1])[0]
            return value, offset + 1

        elif (encoding & 0xF0) == 0xF0:
            # 4-bit immediate integer (1111xxxx)
            value = (encoding & 0x0F) - 1
            return value, offset

        else:
            print(f"Unknown ziplist encoding: 0x{encoding:02x}", file=sys.stderr)
            return None, 1

    def read_listpack(self):
        """Read listpack encoded data"""
        # Use read_string_raw to get the raw bytes
        listpack_bytes = self.read_string_raw()

        # Parse listpack structure
        try:
            return self.parse_listpack(listpack_bytes)
        except Exception as e:
            print(f"Failed to parse listpack ({len(listpack_bytes)} bytes): {e}", file=sys.stderr)
            return []

    def parse_listpack(self, data):
        """Parse listpack binary format"""
        if len(data) < 7:
            print(f"Listpack too short: {len(data)} bytes", file=sys.stderr)
            return []

        # Listpack header: total bytes (4), num elements (2)
        total_bytes = struct.unpack('<I', data[0:4])[0]
        num_elements = struct.unpack('<H', data[4:6])[0]

        result = []
        pos = 6

        for i in range(num_elements):
            if pos >= len(data) - 1:
                break

            try:
                value, bytes_read = self.parse_listpack_entry(data[pos:])
                if value is not None:
                    result.append(value)
                pos += bytes_read
                if bytes_read == 0:
                    break
            except Exception as e:
                print(f"Error parsing listpack entry {i}: {e}", file=sys.stderr)
                break

        return result

    def parse_listpack_entry(self, data):
        """Parse a single listpack entry
        Format: <encoding-type><element-data><element-tot-len>
        The last byte (backlen) indicates the total entry length
        """
        if len(data) < 2:
            return None, 0

        byte = data[0]
        start_pos = 0

        # 7-bit small string (0xxxxxxx) - string with length 0-127
        if byte <= 0x7F:
            length = byte & 0x7F
            if length == 0:
                # Special case: empty string or marker
                # Next byte is backlen
                if len(data) < 2:
                    return None, 0
                backlen = data[1]
                return "", 2

            # String of given length
            if len(data) < 1 + length + 1:
                return None, 0
            value = data[1:1 + length].decode('utf-8', errors='replace')
            # Last byte is backlen (total entry size)
            backlen = data[1 + length]
            total_size = 1 + length + 1
            return value, total_size

        # 6-bit integer (110xxxxx) - integers 0-31
        elif (byte & 0xE0) == 0xC0:
            value = byte & 0x1F
            if len(data) < 2:
                return None, 0
            backlen = data[1]
            return value, 2

        # 13-bit integer (1110xxxx xxxxxxxx)
        elif (byte & 0xF0) == 0xE0:
            if len(data) < 3:
                return None, 0
            value = ((byte & 0x0F) << 8) | data[1]
            # Handle sign
            if value & 0x800:
                value = value - 0x1000
            backlen = data[2]
            return value, 3

        # 12-bit string (10xxxxxx xxxxxxxx)
        elif (byte & 0xC0) == 0x80:
            if len(data) < 2:
                return None, 0
            length = ((byte & 0x3F) << 8) | data[1]
            if len(data) < 2 + length + 1:
                return None, 0
            value = data[2:2 + length].decode('utf-8', errors='replace')
            backlen = data[2 + length]
            total_size = 2 + length + 1
            return value, total_size

        # Special encodings (1111xxxx)
        elif (byte & 0xF0) == 0xF0:
            enc = byte & 0x0F

            # 16-bit integer (11110001)
            if enc == 0x01:
                if len(data) < 4:
                    return None, 0
                value = struct.unpack('<h', data[1:3])[0]
                backlen = data[3]
                return value, 4

            # 24-bit integer (11110010)
            elif enc == 0x02:
                if len(data) < 5:
                    return None, 0
                value_bytes = data[1:4]
                # Sign extend from 24 to 32 bits
                value = int.from_bytes(value_bytes, byteorder='little', signed=False)
                if value & 0x800000:
                    value = value - 0x1000000
                backlen = data[4]
                return value, 5

            # 32-bit integer (11110011)
            elif enc == 0x03:
                if len(data) < 6:
                    return None, 0
                value = struct.unpack('<i', data[1:5])[0]
                backlen = data[5]
                return value, 6

            # 64-bit integer (11110100)
            elif enc == 0x04:
                if len(data) < 10:
                    return None, 0
                value = struct.unpack('<q', data[1:9])[0]
                backlen = data[9]
                return value, 10

            # 32-bit string length (11110000)
            elif enc == 0x00:
                if len(data) < 5:
                    return None, 0
                length = struct.unpack('<I', data[1:5])[0]
                if len(data) < 5 + length + 1:
                    return None, 0
                value = data[5:5 + length].decode('utf-8', errors='replace')
                backlen = data[5 + length]
                total_size = 5 + length + 1
                return value, total_size

            # Unknown encoding
            else:
                print(f"Unknown listpack F-encoding: 0x{byte:02x}", file=sys.stderr)
                return None, 1

        else:
            print(f"Unknown listpack encoding byte: 0x{byte:02x}", file=sys.stderr)
            return None, 1

    def read_stream(self):
        """Read stream data"""
        # Simplified stream reading
        num_elements = self.read_length()
        return f"<stream with {num_elements} elements>"

    def read_value(self, value_type):
        """Read value based on type"""
        if value_type == self.TYPE_STRING:
            return self.read_string()

        elif value_type == self.TYPE_LIST:
            size = self.read_length()
            return [self.read_string() for _ in range(size)]

        elif value_type == self.TYPE_SET:
            size = self.read_length()
            return list(set(self.read_string() for _ in range(size)))

        elif value_type == self.TYPE_ZSET or value_type == self.TYPE_ZSET_2:
            size = self.read_length()
            zset_data = []
            for _ in range(size):
                member = self.read_string()
                score = self.read_double() if value_type == self.TYPE_ZSET else struct.unpack('<d', self.read_bytes(8))[0]
                zset_data.append({"member": member, "score": score})
            return zset_data

        elif value_type == self.TYPE_HASH:
            size = self.read_length()
            hash_data = {}
            for _ in range(size):
                key = self.read_string()
                value = self.read_string()
                hash_data[key] = value
            return hash_data

        elif value_type == self.TYPE_HASH_ZIPMAP:
            zipmap = self.read_string()
            return {f"<zipmap:{len(zipmap)} bytes>": ""}

        elif value_type == self.TYPE_LIST_ZIPLIST:
            return self.read_list_ziplist()

        elif value_type == self.TYPE_SET_INTSET:
            return self.read_set_intset()

        elif value_type == self.TYPE_ZSET_ZIPLIST:
            return self.read_zset_ziplist()

        elif value_type == self.TYPE_HASH_ZIPLIST:
            return self.read_hash_ziplist()

        elif value_type in [self.TYPE_LIST_QUICKLIST, self.TYPE_LIST_QUICKLIST_2]:
            return self.read_quicklist()

        elif value_type in [self.TYPE_HASH_LISTPACK, self.TYPE_ZSET_LISTPACK, self.TYPE_SET_LISTPACK]:
            if value_type == self.TYPE_HASH_LISTPACK:
                return self.read_hash_listpack()
            elif value_type == self.TYPE_ZSET_LISTPACK:
                return self.read_zset_listpack()
            else:
                return self.read_set_listpack()

        elif value_type in [self.TYPE_STREAM_LISTPACKS, self.TYPE_STREAM_LISTPACKS_2, self.TYPE_STREAM_LISTPACKS_3]:
            return self.read_stream()

        else:
            # Unknown type, try to read as string
            try:
                return self.read_string()
            except:
                return f"<unknown type {value_type}>"

    def get_type_name(self, value_type):
        """Get human-readable type name"""
        type_names = {
            self.TYPE_STRING: "string",
            self.TYPE_LIST: "list",
            self.TYPE_SET: "set",
            self.TYPE_ZSET: "zset",
            self.TYPE_ZSET_2: "zset",
            self.TYPE_HASH: "hash",
            self.TYPE_HASH_ZIPMAP: "hash",
            self.TYPE_LIST_ZIPLIST: "list",
            self.TYPE_SET_INTSET: "set",
            self.TYPE_ZSET_ZIPLIST: "zset",
            self.TYPE_HASH_ZIPLIST: "hash",
            self.TYPE_LIST_QUICKLIST: "list",
            self.TYPE_LIST_QUICKLIST_2: "list",
            self.TYPE_HASH_LISTPACK: "hash",
            self.TYPE_ZSET_LISTPACK: "zset",
            self.TYPE_SET_LISTPACK: "set",
            self.TYPE_STREAM_LISTPACKS: "stream",
            self.TYPE_STREAM_LISTPACKS_2: "stream",
            self.TYPE_STREAM_LISTPACKS_3: "stream",
        }
        return type_names.get(value_type, f"unknown_type_{value_type}")

    def parse(self):
        """Parse RDB file"""
        with open(self.filename, 'rb') as f:
            self.file = f

            # Read header
            magic = self.read_bytes(5)
            if magic != b'REDIS':
                raise ValueError(f"Not a valid RDB file. Magic: {magic}")

            # Read version
            version = self.read_bytes(4)
            version_str = version.decode('ascii')
            print(f"RDB Version: {version_str}", file=sys.stderr)

            expiry = None
            idle = None
            freq = None

            while True:
                try:
                    opcode = self.read_byte()

                    if opcode == self.OPCODE_EOF:
                        # Read checksum if present
                        try:
                            checksum = self.read_bytes(8)
                            print(f"Checksum: {checksum.hex()}", file=sys.stderr)
                        except:
                            pass
                        break

                    elif opcode == self.OPCODE_SELECTDB:
                        self.current_db = self.read_length()
                        print(f"Selected DB: {self.current_db}", file=sys.stderr)

                    elif opcode == self.OPCODE_AUX:
                        key = self.read_string()
                        value = self.read_string()
                        self.aux_data[key] = value
                        print(f"AUX: {key} = {value}", file=sys.stderr)

                    elif opcode == self.OPCODE_RESIZEDB:
                        db_size = self.read_length()
                        expires_size = self.read_length()
                        print(f"DB size: {db_size}, Expires: {expires_size}", file=sys.stderr)

                    elif opcode == self.OPCODE_EXPIRETIME_MS:
                        expiry = self.read_unsigned_long()

                    elif opcode == self.OPCODE_EXPIRETIME:
                        expiry = self.read_unsigned_int() * 1000

                    elif opcode == self.OPCODE_IDLE:
                        idle = self.read_length()

                    elif opcode == self.OPCODE_FREQ:
                        freq = self.read_byte()

                    else:
                        # It's a value type opcode
                        value_type = opcode

                        # Only print debug for problematic types
                        if value_type > 21:
                            print(f"Warning: Unexpected type {value_type} at position {self.file.tell()}", file=sys.stderr)

                        try:
                            key = self.read_string()
                            # Only print for invalid keys
                            if key.startswith('<'):
                                print(f"Invalid key: {key[:30]}", file=sys.stderr)
                        except Exception as e:
                            print(f"Error reading key: {e}, skipping entry", file=sys.stderr)
                            continue

                        # Skip invalid keys
                        if not key or key.startswith('<unknown') or key.startswith('<binary') or key.startswith('<parse_error') or key.startswith('<invalid'):
                            print(f"Skipping invalid key: {key}", file=sys.stderr)
                            try:
                                # Try to skip the value
                                self.read_value(value_type)
                            except:
                                pass
                            continue

                        try:
                            value = self.read_value(value_type)

                            if self.simple_format:
                                # Simple format: just the value
                                self.data[key] = value
                            else:
                                # Full format: with metadata
                                entry = {
                                    "value": value,
                                    "type": self.get_type_name(value_type)
                                }

                                if expiry:
                                    entry["expiry_ms"] = expiry
                                    entry["expiry_date"] = datetime.fromtimestamp(expiry / 1000).isoformat()
                                if idle is not None:
                                    entry["idle"] = idle
                                if freq is not None:
                                    entry["freq"] = freq

                                self.data[key] = entry

                            # Reset metadata
                            expiry = None
                            idle = None
                            freq = None

                        except Exception as e:
                            print(f"Error reading value for key '{key[:50]}': {e}", file=sys.stderr)
                            import traceback
                            traceback.print_exc(file=sys.stderr)
                            if not self.simple_format:
                                self.data[key] = {
                                    "error": str(e),
                                    "type": self.get_type_name(value_type)
                                }

                except EOFError:
                    print("Reached end of file", file=sys.stderr)
                    break
                except Exception as e:
                    print(f"Error during parsing: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc(file=sys.stderr)
                    break

        print(f"Parsed {len(self.data)} keys", file=sys.stderr)

        return {
            "rdb_version": version_str,
            "aux": self.aux_data,
            "db": self.current_db,
            "keys": self.data
        }

def main():
    if len(sys.argv) < 2:
        print("Usage: python rdb_parser.py <backup.rdb> [output.json] [options]")
        print("\nOptions:")
        print("  --pretty    Pretty print JSON output")
        print("  --simple    Simple format (values only, no metadata)")
        sys.exit(1)

    rdb_file = sys.argv[1]
    pretty = "--pretty" in sys.argv
    simple = "--simple" in sys.argv
    output_file = None

    for arg in sys.argv[2:]:
        if not arg.startswith("--"):
            output_file = arg

    try:
        parser = RDBParser(rdb_file, simple_format=simple)
        result = parser.parse()

        if simple:
            # In simple mode, just output the keys
            output = result["keys"]
        else:
            output = result

        if pretty:
            json_output = json.dumps(output, indent=2, ensure_ascii=False)
        else:
            json_output = json.dumps(output, ensure_ascii=False)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_output)
            print(f"\nExported to {output_file}", file=sys.stderr)
        else:
            print(json_output)

    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
