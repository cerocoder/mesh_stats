#!/usr/bin/env python3
"""
Meshtastic Relay Node Statistics TUI Application

Interactive pseudo-graphics application for analyzing relay node signal quality.
Displays rxSnr and rxRssi statistics with visual bars for each relay node.
"""

import argparse
import curses
import math
import pickle
import sys
import threading
import time
import meshtastic.serial_interface
import meshtastic.ble_interface
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime
from decimal import Decimal

from pubsub import pub

# Signal bar scale constants (easily modifiable)
SNR_SCALE_MIN = -20   # dB
SNR_SCALE_MAX = +10   # dB
RSSI_SCALE_MIN = -120 # dBm
RSSI_SCALE_MAX = -60  # dBm

# Bar display width in characters
BAR_WIDTH = 40

# Time window (seconds) for "just received" flash effect (inverse color)
FLASH_DURATION = 0.5

# Spinner characters for packet receive indicator
SPINNER_CHARS = ['|', '/', '-', '\\']

# Color pair IDs (defined as constants for easy modification)
# These are initialized in MeshStatsTUI.init_colors()
COLOR_SNR_BAR = 1              # SNR bar background fill
COLOR_SNR_INDICATOR = 2        # SNR bar indicators (| and *)
COLOR_SNR_INDICATOR_FLASH = 10 # SNR bar indicator flash color (*)
COLOR_RSSI_BAR = 8             # RSSI bar background fill
COLOR_RSSI_INDICATOR = 9       # RSSI bar indicators (| and *)
COLOR_RSSI_INDICATOR_FLASH = 11 # RSSI bar indicator flash color (*)
COLOR_SELECTED = 3             # Selected row highlight
COLOR_HEADER = 4               # Header text
COLOR_STATUS_BAR = 5           # Status/help bar
COLOR_PAUSED = 6               # Paused indicator
COLOR_NORMAL = 7               # Normal text
COLOR_MY_NODE = 12             # Local node info line (highlight)

# Sorting modes
SORT_MODES = [
    ("pkts", "Packet count"),
    ("pct", "Percentage"),
    ("snr", "Avg rxSnr"),
    ("rssi", "Avg rxRssi"),
    ("name", "Node name"),
]

# Visualization modes for statistics bars
VIS_MODE_SIMPLE = 0   # Just show current level as a bar
VIS_MODE_COMPLEX = 1  # Show min/max range, average, and last value indicators
VIS_MODES = [
    ("simple", "Simple"),
    ("complex", "Complex"),
]


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points on Earth using Haversine formula.

    Args:
        lat1, lon1: Latitude and longitude of first point (degrees)
        lat2, lon2: Latitude and longitude of second point (degrees)

    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth's radius in kilometers

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


@dataclass
class SignalStats:
    """Statistics for a single signal metric (SNR or RSSI)."""
    min_val: float = float('inf')
    max_val: float = float('-inf')
    sum_val: float = 0.0
    count: int = 0
    last_val: float = 0.0

    def update(self, value: float) -> None:
        """Update statistics with a new value."""
        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)
        self.sum_val += value
        self.count += 1
        self.last_val = value

    @property
    def avg(self) -> float:
        """Calculate average value."""
        return self.sum_val / self.count if self.count > 0 else 0.0

    def reset(self) -> None:
        """Reset all statistics."""
        self.min_val = float('inf')
        self.max_val = float('-inf')
        self.sum_val = 0.0
        self.count = 0
        self.last_val = 0.0


@dataclass
class SignalHistoryStat(SignalStats):
    """Extends SignalStats with full (timestamp, value) history. Timestamp is passed in by caller (local time)."""
    history: List[Tuple[float, float]] = field(default_factory=list)

    def update(self, timestamp: float, value: float) -> None:
        """Update with a new (timestamp, value). Caller passes local time."""
        super().update(value)
        self.history.append((timestamp, value))

    def reset(self) -> None:
        """Reset all statistics and history."""
        super().reset()
        self.history.clear()


@dataclass
class PositionMessage:
    """A single position message received from a node."""
    timestamp: float  # When the position was received
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[int] = None
    precision_bits: Optional[int] = None
    raw: Dict = None

    def __init__(self, allraw: Dict):
        raw = dict(allraw)
        if "latitudeI" in raw:
            raw["latitude"] = float(raw["latitudeI"] * Decimal("1e-7"))
        if "longitudeI" in raw:
            raw["longitude"] = float(raw["longitudeI"] * Decimal("1e-7"))
        self.raw = raw
        self.timestamp = time.time()
        self.latitude = raw.get("latitude")
        self.longitude = raw.get("longitude")
        # Altitude: from Meshtastic Position protobuf.
        # - "altitude" = meters above MSL (Mean Sea Level), legacy field.
        # - "altitude_hae" = Height Above Ellipsoid (WGS84), preferred when present.
        # Priority: use altitude_hae first, then altitude (MSL).
        self.altitude = None
        for key in ("altitude_hae", "altitude"):
            if key in raw and raw.get(key) is not None:
                self.altitude = int(raw[key])
                break
        self.precision_bits = raw.get("precisionBits") or raw.get("precision_bits")

    def has_coordinates(self) -> bool:
        """Check if this position has valid coordinates."""
        return self.latitude is not None and self.longitude is not None

    def has_altitude(self) -> bool:
        """Check if this position has valid altitude."""
        return self.altitude is not None


@dataclass
class NodePositionHistory:
    """History of position messages for a single node."""
    node_num: int
    positions: List['PositionMessage'] = field(default_factory=list)

    def add_position(self, position: PositionMessage) -> None:
        """Add a new position to the history."""
        self.positions.append(position)

    @property
    def last_position(self) -> Optional[PositionMessage]:
        """Get the most recent position message."""
        return self.positions[-1] if self.positions else None

    @property
    def best_position(self) -> Optional[PositionMessage]:
        """Get the best topical position."""
        def good_pos(p):
            return p.has_coordinates() and p.has_altitude()
        # return last_position 
        last_pos = self.last_position
        if last_pos:
            if good_pos(last_pos):
                return last_pos
        for pos in reversed(self.positions):
            if good_pos(pos):
                return pos
        return None

    @property
    def count(self) -> int:
        """Get number of position messages stored."""
        return len(self.positions)


@dataclass
class RemoteNodeStats:
    """Statistics for a remote node (packets received from this node via a relay)."""
    packet_count: int = 0
    hops_made_sum: float = 0.0
    hops_made_count: int = 0
    hops_left_sum: float = 0.0
    hops_left_count: int = 0

    def update(self, hop_start: Optional[int], hop_limit: Optional[int]) -> None:
        """Update statistics with new hop data."""
        self.packet_count += 1
        if hop_start is not None and hop_limit is not None:
            hops_made = hop_start - hop_limit
            self.hops_made_sum += hops_made
            self.hops_made_count += 1
            self.hops_left_sum += hop_limit
            self.hops_left_count += 1

    @property
    def avg_hops_made(self) -> Optional[float]:
        """Average number of hops made."""
        if self.hops_made_count == 0:
            return None
        return self.hops_made_sum / self.hops_made_count

    @property
    def avg_hops_left(self) -> Optional[float]:
        """Average number of hops left."""
        if self.hops_left_count == 0:
            return None
        return self.hops_left_sum / self.hops_left_count


@dataclass
class NodeTelemetryRecord:
    """Per-node telemetry: uptime/restart count and history_metrics dict (key -> SignalHistoryStat)."""
    last_uptime_seconds: Optional[int] = None
    observed_restart_count: int = 0
    history_metrics: Dict[str, SignalHistoryStat] = field(default_factory=dict)


@dataclass
class RelayNodeStats:
    """Statistics for a single relay node.

    Note: relay_node_byte is the last byte of the node's full NodeNum.
    Multiple nodes in the database could match the same relay_node_byte.
    """
    relay_node_byte: int  # The relayNode field value (last byte of NodeNum)
    node_name: str = ""   # Name if exactly one match found, empty otherwise
    snr: SignalStats = field(default_factory=SignalStats)
    rssi: SignalStats = field(default_factory=SignalStats)
    packet_count: int = 0
    first_packet_time: float = 0.0
    last_packet_time: float = 0.0
    # Remote node number (packet "from") -> statistics for that remote node
    from_node_stats: Dict[int, RemoteNodeStats] = field(default_factory=dict)

    @property
    def hex_id(self) -> str:
        """Get hex representation of relay node byte."""
        return f"0x{self.relay_node_byte:02x}"

    @property
    def known_nodes_count(self) -> int:
        """Number of distinct remote nodes whose packets were retransmitted via this relay."""
        return len(self.from_node_stats)

    def update(self, rx_snr: Optional[float], rx_rssi: Optional[int],
               from_node: Optional[int] = None,
               hop_start: Optional[int] = None,
               hop_limit: Optional[int] = None) -> None:
        """Update statistics with new packet data."""
        now = time.time()
        if self.packet_count == 0:
            self.first_packet_time = now
        self.last_packet_time = now
        self.packet_count += 1
        if from_node is not None:
            if from_node not in self.from_node_stats:
                self.from_node_stats[from_node] = RemoteNodeStats()
            self.from_node_stats[from_node].update(hop_start, hop_limit)

        if rx_snr is not None:
            self.snr.update(rx_snr)
        if rx_rssi is not None:
            self.rssi.update(float(rx_rssi))

    @property
    def packets_per_hour(self) -> float:
        """Calculate packets per hour rate."""
        if self.packet_count < 2:
            return 0.0
        duration = self.last_packet_time - self.first_packet_time
        if duration <= 0:
            return 0.0
        return self.packet_count / duration * 3600

    @property
    def just_received(self) -> bool:
        """Check if a packet was received within the flash duration."""
        if self.last_packet_time == 0:
            return False
        return (time.time() - self.last_packet_time) < FLASH_DURATION

    def reset(self) -> None:
        """Reset all statistics."""
        self.snr.reset()
        self.rssi.reset()
        self.packet_count = 0
        self.first_packet_time = 0.0
        self.last_packet_time = 0.0
        self.from_node_stats.clear()


class StatsCollector:
    """Collects and manages statistics for all relay nodes."""

    def __init__(self, meshview_url: Optional[str] = None):
        self.nodes: Dict[int, RelayNodeStats] = {}
        self.total_packets: int = 0
        self.total_relayed_packets: int = 0
        self.paused: bool = False
        self.lock = threading.Lock()
        self.interface = None
        self.sort_mode: int = 0  # Index into SORT_MODES
        self.meshview_url: Optional[str] = meshview_url.rstrip('/') if meshview_url else None
        self.db_load_time: Optional[float] = None  # Timestamp when node DB was loaded
        self.spinner_index: int = 0  # Current spinner character index
        self.last_packet_time: Optional[float] = None  # Timestamp of last packet received
        self.last_relayed_packet_time: Optional[float] = None  # Timestamp of last relayed packet
        # Snapshot of interface.nodesByNum taken after config load; avoids threading issues
        self._nodes_by_num: Dict = {}
        # Position history for each node: node_num -> NodePositionHistory
        self._node_positions: Dict[int, NodePositionHistory] = {}
        # Local node position for replay mode (lat, lon) or None
        self._local_node_position: Optional[Tuple[float, float]] = None
        # Dictionary of nodes that are skiiped from routers, the node num is a key
        # the value is str: persist, user.
        self.skip_relays: Dict[int, str] = {}
        # Per-node telemetry (node_num -> NodeTelemetryRecord)
        self._node_telemetry: Dict[int, NodeTelemetryRecord] = {}
        # Last LocalStats from local device (when TELEMETRY_APP from my node)
        self._local_stats_last: Optional[Dict] = None

    def add_skipped_relay_node(self, node_num: int, skip_value="persist") -> None:
        """Add node to the skipped node list. These nodes cannot be tractated as relays"""
        self.skip_relays[node_num] = skip_value
        self.refresh_relay_node_name(node_num & 0xFF)

    def rem_skipped_relay_node(self, node_num: int) -> None:
        """Remove the node_num key from skip_routers, only if added by a user"""
        if self.skip_relays.get(node_num, "user") == "user":
            self.skip_relays.pop(node_num, None)
            self.refresh_relay_node_name(node_num & 0xFF)

    def _update_relay_node_name_held(self, relay_byte: int) -> None:
        """Update self.nodes[relay_byte].node_name. Call only with self.lock held."""
        if relay_byte in self.nodes:
            self.nodes[relay_byte].node_name = self.get_node_name(relay_byte)

    def refresh_relay_node_name(self, relay_byte: int) -> None:
        """Update self.nodes[relay_byte].node_name with lock obtained."""
        with self.lock:
            self._update_relay_node_name_held(relay_byte)

    def _wait_for_node_db_stable(
        self,
        poll_interval: float = 0.4,
        stable_interval: float = 1.2,
        timeout: float = 15.0,
    ) -> None:
        """Wait until interface.nodesByNum count is stable (no new nodes for stable_interval).

        waitForConfig() returns when device config is ready; node DB can still be
        streaming in. This waits until the count stops changing before we snapshot.
        """
        deadline = time.time() + timeout
        last_count = -1
        count_stable_since = time.time()
        while time.time() < deadline:
            n = len(self.interface.nodesByNum)
            if n == last_count:
                if time.time() - count_stable_since >= stable_interval:
                    return
            else:
                last_count = n
                count_stable_since = time.time()
            time.sleep(poll_interval)

    def reload_node_database(self) -> bool:
        """Reload the node database from the connected device.

        Takes a snapshot of interface.nodesByNum after config and node DB are stable
        so all lookups use an independent copy and avoid multithreading issues.
        Returns True if successful, False otherwise.
        """
        if self.interface is None:
            return False
        try:
            # Request fresh config from device
            self.interface._startConfig()
            # Wait for config to complete
            self.interface.waitForConfig()
            # Wait for node DB to stop growing (nodes stream in after config)
            self._wait_for_node_db_stable()
            # Snapshot nodesByNum under lock; use this copy for all lookups
            with self.lock:
                self._nodes_by_num = dict(self.interface.nodesByNum)
                self.db_load_time = time.time()
                relay_bytes = list(self.nodes.keys())
            for relay_byte in relay_bytes:
                self.refresh_relay_node_name(relay_byte)
            return True
        except Exception as e:
            print(f"Error reloading node database: {e}")
            return False

    def load_node_database(self, nodes_by_num: Dict, db_load_time: Optional[float] = None,
                           local_position: Optional[Tuple[float, float]] = None) -> None:
        """Load node database from saved data (for replay mode).

        Args:
            nodes_by_num: Dictionary of node_num -> node_info
            db_load_time: Original timestamp when DB was saved
            local_position: Local node position (lat, lon) or None
        """
        with self.lock:
            self._nodes_by_num = dict(nodes_by_num)
            self.db_load_time = db_load_time
            self._local_node_position = local_position

    def get_db_load_time_str(self) -> str:
        """Get formatted DB load time string."""
        if self.db_load_time is None:
            return "N/A"
        dt = datetime.fromtimestamp(self.db_load_time)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def get_db_cnt(self) -> int:
        """ Get count of records in the local node DB """
        return len(self._nodes_by_num)

    def get_local_node_position(self) -> Optional[Tuple[float, float]]:
        """Get local node's position (latitude, longitude).

        Uses get_node_location_info(local_node_num, None) so position comes from
        _node_positions or _nodes_by_num like other nodes. Replay cache is checked first.

        Returns:
            Tuple of (latitude, longitude) or None if not available
        """
        if self._local_node_position is not None:
            return self._local_node_position

        node_num = None
        if self.interface is not None:
            try:
                my_info = self.interface.getMyNodeInfo()
                if my_info:
                    node_num = my_info.get("num")
            except Exception:
                pass
        if node_num is None:
            return None
        loc = self.get_node_location_info(node_num, None)
        lat, lon = loc.get("lat"), loc.get("lon")
        if lat is not None and lon is not None:
            return (lat, lon)
        return None

    def set_local_node_position(self, position: Optional[Tuple[float, float]]) -> None:
        """Set the local node position (for replay mode).

        Args:
            position: Tuple of (latitude, longitude) or None
        """
        self._local_node_position = position

    def get_spinner_char(self) -> str:
        """Get current spinner character."""
        with self.lock:
            return SPINNER_CHARS[self.spinner_index]

    def get_last_packet_time_str(self) -> str:
        """Get formatted last packet time string."""
        with self.lock:
            if self.last_packet_time is None:
                return "N/A"
            from datetime import datetime
            dt = datetime.fromtimestamp(self.last_packet_time)
            return dt.strftime("%H:%M:%S")

    def get_last_relayed_packet_time_str(self) -> str:
        """Get formatted last relayed packet time string."""
        with self.lock:
            if self.last_relayed_packet_time is None:
                return "N/A"
            from datetime import datetime
            dt = datetime.fromtimestamp(self.last_relayed_packet_time)
            return dt.strftime("%H:%M:%S")

    def set_interface(self, interface) -> None:
        """Set the meshtastic interface for node info lookups."""
        self.interface = interface

    def get_last_byte_of_node_num(self, node_num: int) -> int:
        """Get last byte of node number, following firmware logic.

        Returns (node_num & 0xFF), or 0xFF if that would be 0.
        """
        last_byte = node_num & 0xFF
        return 0xFF if last_byte == 0 else last_byte

    def find_matching_nodes(self, relay_node_byte: int) -> List[Dict]:
        """Find all nodes in database whose last byte matches relay_node_byte.

        Uses the snapshot _nodes_by_num taken in reload_node_database() to avoid
        multithreading issues with the live interface.nodesByNum.
        Args:
            relay_node_byte: The relayNode field value (last byte of relaying node's number)

        Returns:
            List of node info dicts that match
        """
        matches = []
        # Use snapshot; no lock needed (reference read is safe; dict is only replaced in reload_node_database)
        nodes_snapshot = self._nodes_by_num
        for node_num, node_info in nodes_snapshot.items():
            if self.get_last_byte_of_node_num(node_num) == relay_node_byte:
                if not self.skip_relays.get(node_num, False):
                    matches.append(node_info)
        return matches

    def get_node_name(self, relay_node_byte: int) -> str:
        """Get node name if exactly one node matches the relay byte.

        Args:
            relay_node_byte: The relayNode field value (last byte)

        Returns:
            Short name if exactly one match, empty string otherwise
        """
        matches = self.find_matching_nodes(relay_node_byte)
        if len(matches) == 1:
            node_info = matches[0]
            if "user" in node_info:
                short_name = node_info["user"].get("shortName", "")
                if short_name:
                    return short_name
        return ""

    def get_relay_node_hex(self, relay_node_byte: int) -> str:
        """Get hex representation of relay node byte."""
        return f"0x{relay_node_byte:02x}"

    def get_node_info(self, relay_node_byte: int) -> Optional[Dict]:
        """Get full node info if exactly one match exists."""
        matches = self.find_matching_nodes(relay_node_byte)
        if len(matches) == 1:
            return matches[0]
        return None

    def store_position(self, node_num: int, position_data: Dict) -> None:
        """Store a position message for a node.

        Args:
            node_num: The node number that sent the position
            position_data: The decoded position dictionary
        """
        position = PositionMessage(position_data)
        with self.lock:
            if node_num not in self._node_positions:
                self._node_positions[node_num] = NodePositionHistory(node_num=node_num)
            self._node_positions[node_num].add_position(position)

    def get_node_position_history(self, node_num: int) -> Optional[NodePositionHistory]:
        """Get position history for a node.

        Args:
            node_num: Full node number

        Returns:
            NodePositionHistory or None if no positions received
        """
        with self.lock:
            return self._node_positions.get(node_num)

    def get_best_received_position(self, node_num: int) -> Optional[PositionMessage]:
        """Get the last received position for a node.

        Args:
            node_num: Full node number

        Returns:
            Last PositionMessage or None if no positions received
        """
        history = self.get_node_position_history(node_num)
        return history.best_position if history else None

    def _obfuscation_radius_meters(self, precision_bits: Optional[int]) -> Optional[float]:
        """Meshtastic position obfuscation radius in meters from precision_bits.

        Uses the standard relationship: radius decreases by ~half per bit.
        Returns None if precision_bits is None or 0 (unknown/location not sent).
        """
        if precision_bits is None or precision_bits <= 0:
            return None
        return 23905784.0 / (2 ** precision_bits)

    def _bearing_to_direction(self, bearing_deg: float) -> str:
        """Map bearing in degrees (0=N, 90=E, 180=S, 270=W) to N, NE, E, SE, S, SW, W, NW."""
        bearing_deg = bearing_deg % 360.0
        sector = (bearing_deg + 22.5) % 360 // 45
        return ("N", "NE", "E", "SE", "S", "SW", "W", "NW")[int(sector)]

    def get_node_location_info(
        self,
        node_num: int,
        current_position: Optional[Tuple[float, float]] = None,
    ) -> Dict:
        """Return a dict with altitude, distance, obfuscation radius, and direction for a node.

        Uses self._node_positions first (higher priority), then self._nodes_by_num.
        If obfuscation radius is greater than distance to the node, direction is "un"
        (unknown) because the position uncertainty exceeds the distance.

        Args:
            node_num: Full node number (as in nodesByNum / position sender).
            current_position: (latitude, longitude) of the local/current position in degrees, is not mandatory

        Returns:
            Dict with keys:
            - altitude: int (meters) or None if not available
            - distance: float (km) or None if position not available
            - obfuscation_radius: float (meters) or None if precision_bits not available
            - direction: str one of N, NE, E, SE, S, SW, W, NW, or "un"
            - lat: latitude
            - lon: longitude
            - src: source of the data one of DB, CUR
            - timestamp: when the data was acquired (from DB lastHeard or position message timestamp)
        """
        result = {
            "altitude": None,
            "distance": None,
            "obfuscation_radius": None,
            "direction": "un",
            "lat": None,
            "lon": None,
            "src": None,
            "timestamp": None,
        }
        local_lat, local_lon = current_position if current_position else (None, None)
        node_lat, node_lon = None, None
        altitude = None
        precision_bits = None

        # Prefer _node_positions: use most recent position with valid coords (last to first)
        history = self._node_positions.get(node_num)
        pos = history.best_position if history else None

        # Fallback to _nodes_by_num
        if pos is None:
            node_info = self._nodes_by_num.get(node_num)
            if node_info and "position" in node_info:
                pos = node_info["position"]
                node_lat = pos.get("latitude")
                node_lon = pos.get("longitude")
                result["src"] = "DB"
                result["timestamp"] = node_info.get("lastHeard")
                if altitude is None and pos.get("altitude") is not None:
                    altitude = int(pos.get("altitude"))
                # DB usually does not have precision_bits; leave None
                precision_bits = None
        else:
            node_lat, node_lon, altitude = pos.latitude, pos.longitude, pos.altitude
            precision_bits = pos.precision_bits
            result["src"] = "CUR"
            result["timestamp"] = getattr(pos, "timestamp", None)

        result["altitude"] = altitude

        if node_lat is None or node_lon is None:
            return result

        obf_radius = self._obfuscation_radius_meters(precision_bits)
        result["obfuscation_radius"] = obf_radius

        if local_lat is not None and local_lon is not None:
            distance_km = haversine_distance(local_lat, local_lon, node_lat, node_lon)
            result["distance"] = distance_km
            if obf_radius is not None and distance_km is not None and obf_radius >= (distance_km * 1000.0):
                result["direction"] = "un"
            else:
                # Bearing from current position to node (degrees, 0=N, 90=E)
                dlon_rad = math.radians(node_lon - local_lon)
                lat1_rad = math.radians(local_lat)
                lat2_rad = math.radians(node_lat)
                y = math.sin(dlon_rad) * math.cos(lat2_rad)
                x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
                bearing_deg = math.degrees(math.atan2(y, x)) % 360.0
                result["direction"] = self._bearing_to_direction(bearing_deg)

        result["lat"], result["lon"] = node_lat, node_lon
        
        return result

    def _get_telemetry_val(self, d: Dict, key: str, default=None):
        """Get value from dict trying key and camelCase variant."""
        if d is None:
            return default
        if key in d:
            return d[key]
        parts = key.split("_")
        camel = parts[0].lower() + "".join(p.capitalize() for p in parts[1:])
        return d.get(camel, default)

    def _process_telemetry_packet(self, node_num: int, decoded: Dict, timestamp: float) -> None:
        """Process TELEMETRY_APP packet: update _node_telemetry and optionally _local_stats_last."""
        telemetry = decoded.get("telemetry") or decoded
        with self.lock:
            if node_num not in self._node_telemetry:
                self._node_telemetry[node_num] = NodeTelemetryRecord()
            rec = self._node_telemetry[node_num]

            # DeviceMetrics
            dm = telemetry.get("device_metrics") or telemetry.get("deviceMetrics")
            if dm is not None:
                uptime = self._get_telemetry_val(dm, "uptime_seconds")
                if uptime is not None:
                    try:
                        uptime = int(uptime)
                        if rec.last_uptime_seconds is not None and uptime < rec.last_uptime_seconds:
                            rec.observed_restart_count += 1
                        rec.last_uptime_seconds = uptime
                    except (TypeError, ValueError):
                        pass
                for key, proto_key in (
                    ("battery_level", "battery_level"),
                    ("voltage", "voltage"),
                    ("channel_utilization", "channel_utilization"),
                    ("air_util_tx", "air_util_tx"),
                ):
                    val = self._get_telemetry_val(dm, proto_key)
                    if val is not None:
                        try:
                            v = float(val)
                            if key not in rec.history_metrics:
                                rec.history_metrics[key] = SignalHistoryStat()
                            rec.history_metrics[key].update(timestamp, v)
                        except (TypeError, ValueError):
                            pass

            # EnvironmentMetrics
            em = telemetry.get("environment_metrics") or telemetry.get("environmentMetrics")
            if em is not None:
                for key, proto_key in (
                    ("temperature", "temperature"),
                    ("voltage", "voltage"),
                    ("current", "current"),
                ):
                    val = self._get_telemetry_val(em, proto_key)
                    if val is not None:
                        try:
                            v = float(val)
                            if key not in rec.history_metrics:
                                rec.history_metrics[key] = SignalHistoryStat()
                            rec.history_metrics[key].update(timestamp, v)
                        except (TypeError, ValueError):
                            pass

            # PowerMetrics
            pm = telemetry.get("power_metrics") or telemetry.get("powerMetrics")
            if pm is not None:
                for ch in range(1, 9):
                    for suf, proto in (("voltage", f"ch{ch}_voltage"), ("current", f"ch{ch}_current")):
                        key = f"ch{ch}_{suf}"
                        val = self._get_telemetry_val(pm, proto)
                        if val is not None:
                            try:
                                v = float(val)
                                if key not in rec.history_metrics:
                                    rec.history_metrics[key] = SignalHistoryStat()
                                rec.history_metrics[key].update(timestamp, v)
                            except (TypeError, ValueError):
                                pass

            # LocalStats: store only when packet is from the local device
            ls = telemetry.get("local_stats") or telemetry.get("localStats")
            if ls is not None:
                my_info = getattr(self.interface, "myInfo", None) if self.interface else None
                my_node = getattr(my_info, "my_node_num", None) if my_info else None
                if my_node is not None and node_num == my_node:
                    self._local_stats_last = dict(ls)

    def on_receive(self, packet, interface) -> None:
        """Handle received packet from meshtastic.

        Updates spinner and timestamps for all packets.
        Stores position messages for each node.
        Only updates relay node statistics for packets that have a relayNode attribute.
        """
        current_time = time.time()

        # Paused means no packet received, all of them are dropped
        if self.paused:
            return

        # Always update spinner and last packet time (even when paused)
        with self.lock:
            self.spinner_index = (self.spinner_index + 1) % len(SPINNER_CHARS)
            self.last_packet_time = current_time
            self.total_packets += 1

        # Check for position packets and store them (even when paused)
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum")
        from_node = packet.get("from")
        if portnum == "POSITION_APP" and from_node is not None:
            position_data = decoded.get("position", {})
            if position_data:
                self.store_position(from_node, position_data)

        if portnum == "TELEMETRY_APP" or portnum == 67:
            if from_node is not None:
                self._process_telemetry_packet(from_node, decoded, current_time)

        # Only handle relay stats for packets with relayNode attribute
        relay_node_byte = packet.get("relayNode")
        if relay_node_byte is None:
            return

        # Skip invalid relay node (NO_RELAY_NODE = 0 means no relay info)
        if relay_node_byte == 0:
            return

        rx_snr = packet.get("rxSnr")
        rx_rssi = packet.get("rxRssi")
        # from_node = packet.get("from"), already got, see above
        hop_start = packet.get("hopStart")
        hop_limit = packet.get("hopLimit")

        # Skip packet if received from a node declared as non-relay (skip_relays), when it
        # looks like a directly received packet. Use hop_start/hop_limit when present:
        # one hop made (hop_start - hop_limit == 1) means we are the first receiver.
        if self.skip_relays.get(from_node):
            if hop_start is not None and hop_limit is not None:
                hops_made = hop_start - hop_limit
                if hops_made != 1:
                    pass  # multi-hop: do not skip, count as relayed
                else:
                    return  # direct (one hop): skip
            else:
                return  # no hop info: keep legacy behavior, skip

        with self.lock:
            self.total_relayed_packets += 1
            self.last_relayed_packet_time = current_time

            if relay_node_byte not in self.nodes:
                self.nodes[relay_node_byte] = RelayNodeStats(
                    relay_node_byte=relay_node_byte,
                    node_name=self.get_node_name(relay_node_byte)
                )

            self.nodes[relay_node_byte].update(
                rx_snr, rx_rssi,
                from_node=from_node,
                hop_start=hop_start,
                hop_limit=hop_limit
            )
            self._update_relay_node_name_held(relay_node_byte)

    def toggle_pause(self) -> None:
        """Toggle pause state."""
        with self.lock:
            self.paused = not self.paused

    def reset(self) -> None:
        """Reset all statistics."""
        with self.lock:
            self.nodes.clear()
            self.total_packets = 0
            self.total_relayed_packets = 0
            self.spinner_index = 0
            self.last_packet_time = None
            self.last_relayed_packet_time = None
            self._node_positions.clear()
            self._node_telemetry.clear()
            self._local_stats_last = None

    def cycle_sort_mode(self) -> None:
        """Cycle to next sort mode."""
        with self.lock:
            self.sort_mode = (self.sort_mode + 1) % len(SORT_MODES)

    def get_sorted_nodes(self) -> List[RelayNodeStats]:
        """Get list of nodes sorted by current sort mode."""
        with self.lock:
            nodes = list(self.nodes.values())
            total = self.total_relayed_packets

        mode = SORT_MODES[self.sort_mode][0]

        if mode == "pkts":
            nodes.sort(key=lambda n: n.packet_count, reverse=True)
        elif mode == "pct":
            nodes.sort(key=lambda n: n.packet_count / total if total > 0 else 0, reverse=True)
        elif mode == "snr":
            nodes.sort(key=lambda n: n.snr.avg if n.snr.count > 0 else float('-inf'), reverse=True)
        elif mode == "rssi":
            nodes.sort(key=lambda n: n.rssi.avg if n.rssi.count > 0 else float('-inf'), reverse=True)
        elif mode == "name":
            # Sort by name if available, otherwise by hex id
            nodes.sort(key=lambda n: n.node_name if n.node_name else n.hex_id)

        return nodes

    def get_total_packets(self) -> int:
        """Get total packet count (all packets)."""
        with self.lock:
            return self.total_packets

    def get_total_relayed_packets(self) -> int:
        """Get total relayed packet count."""
        with self.lock:
            return self.total_relayed_packets

    def is_paused(self) -> bool:
        """Check if collection is paused."""
        with self.lock:
            return self.paused


class MeshStatsTUI:
    """Curses-based TUI for displaying relay node statistics."""

    def __init__(self, stats: StatsCollector, replay_mode: bool = False):
        self.stats = stats
        self.replay_mode = replay_mode
        self.selected_index: int = 0
        self.scroll_offset: int = 0
        self.show_details: bool = False
        self.detail_scroll_offset: int = 0  # Scroll position in detail view
        self.running: bool = True
        self.stdscr = None
        self.vis_mode: int = VIS_MODE_SIMPLE  # Default to simple visualization
        # Cached detail lines to avoid expensive rebuilds on every render
        self._detail_lines_cache: Optional[List[Tuple[str, int]]] = None
        self._detail_cache_node_byte: Optional[int] = None

    def init_colors(self) -> None:
        """Initialize color pairs for the TUI.

        Color pairs are defined using constants at the top of the file
        for easy modification of the color scheme.
        """
        curses.start_color()
        curses.use_default_colors()

        # SNR bar colors (green theme)
        # COLOR_SNR_BAR: Bar fill (green foreground, shown as block chars)
        curses.init_pair(COLOR_SNR_BAR, curses.COLOR_GREEN, -1)
        # COLOR_SNR_INDICATOR: Indicators on SNR bar (black on green background)
        curses.init_pair(COLOR_SNR_INDICATOR, curses.COLOR_BLACK, curses.COLOR_GREEN)
        # COLOR_SNR_INDICATOR_FLASH: Flash color for '*' on SNR bar (white on green)
        curses.init_pair(COLOR_SNR_INDICATOR_FLASH, curses.COLOR_WHITE, curses.COLOR_GREEN)

        # RSSI bar colors (yellow theme)
        # COLOR_RSSI_BAR: Bar fill (yellow foreground, shown as block chars)
        curses.init_pair(COLOR_RSSI_BAR, curses.COLOR_YELLOW, -1)
        # COLOR_RSSI_INDICATOR: Indicators on RSSI bar (black on yellow background)
        curses.init_pair(COLOR_RSSI_INDICATOR, curses.COLOR_BLACK, curses.COLOR_YELLOW)
        # COLOR_RSSI_INDICATOR_FLASH: Flash color for '*' on RSSI bar (white on yellow)
        curses.init_pair(COLOR_RSSI_INDICATOR_FLASH, curses.COLOR_WHITE, curses.COLOR_YELLOW)

        # COLOR_SELECTED: Selected row highlight
        curses.init_pair(COLOR_SELECTED, curses.COLOR_BLACK, curses.COLOR_CYAN)
        # COLOR_HEADER: Header text
        curses.init_pair(COLOR_HEADER, curses.COLOR_YELLOW, -1)
        # COLOR_STATUS_BAR: Status/help bar
        curses.init_pair(COLOR_STATUS_BAR, curses.COLOR_BLACK, curses.COLOR_WHITE)
        # COLOR_PAUSED: Paused indicator
        curses.init_pair(COLOR_PAUSED, curses.COLOR_RED, -1)
        # COLOR_NORMAL: Normal text
        curses.init_pair(COLOR_NORMAL, curses.COLOR_WHITE, -1)
        # COLOR_MY_NODE: Local node info highlight
        curses.init_pair(COLOR_MY_NODE, curses.COLOR_CYAN, -1)

    def render_my_info(self, win, row: int, max_width: int) -> None:
        """Render one line with local node short name and geo info (position oneline), above headers.

        Gets node_num from getMyNodeInfo(), then get_node_location_info(node_num, current_pos)
        and uses the returned loc_info for full geo display via render_position_oneline().
        """
        short_name = ""
        node_num = None
        if self.stats.interface:
            try:
                my_info = self.stats.interface.getMyNodeInfo()
                if my_info:
                    short_name = (my_info.get("user") or {}).get("shortName", "") or ""
                    node_num = my_info.get("num")
            except Exception:
                pass
        if node_num is not None:
            loc_info = self.stats.get_node_location_info(node_num, None)
            pos_line = self.render_position_oneline(node_num, loc_info, "")
        else:
            pos_line = "No local node info"
        try:
            line = f"{short_name} {pos_line}"
            win.addstr(row, 0, line, curses.color_pair(COLOR_MY_NODE) | curses.A_BOLD)
        except curses.error:
            pass

    def value_to_bar_position(self, value: float, scale_min: float, scale_max: float) -> int:
        """Convert a value to bar position (0 to BAR_WIDTH-1)."""
        if value <= scale_min:
            return 0
        if value >= scale_max:
            return BAR_WIDTH - 1
        ratio = (value - scale_min) / (scale_max - scale_min)
        return int(ratio * (BAR_WIDTH - 1))

    def render_bar_simple(self, win, y: int, x: int, stats: SignalStats,
                          scale_min: float, scale_max: float,
                          color_bar: int) -> None:
        """Render a simple signal bar showing only the current level.

        Args:
            win: Curses window to draw on
            y, x: Position to draw at
            stats: Signal statistics to visualize
            scale_min, scale_max: Fixed scale range
            color_bar: Color pair ID for bar fill
        """
        if stats.count == 0:
            # No data yet
            bar_str = " " * BAR_WIDTH
            win.addstr(y, x, bar_str)
            return

        # Calculate position of last value
        last_pos = self.value_to_bar_position(stats.last_val, scale_min, scale_max)

        # Build the bar: fill from left edge to last_pos
        for i in range(BAR_WIDTH):
            if i <= last_pos:
                char = '█'
                attr = curses.color_pair(color_bar)
            else:
                char = ' '
                attr = curses.color_pair(COLOR_NORMAL)

            try:
                win.addch(y, x + i, char, attr)
            except curses.error:
                pass  # Ignore errors at screen edge

    def render_bar_complex(self, win, y: int, x: int, stats: SignalStats,
                           scale_min: float, scale_max: float,
                           color_bar: int, color_indicator: int, color_indicator_flash: int,
                           flash_last: bool = False, fill_from_zero: bool = False) -> None:
        """Render a complex signal bar with min/max range, average and last indicators.

        Args:
            win: Curses window to draw on
            y, x: Position to draw at
            stats: Signal statistics to visualize
            scale_min, scale_max: Fixed scale range
            color_bar: Color pair ID for bar fill
            color_indicator: Color pair ID for indicators (| and *)
            color_indicator_flash: Color pair ID for '*' when flashing
            flash_last: If True, show '*' (last value) in flash color
            fill_from_zero: If True, fill bar from left edge to last_pos (like simple mode)
        """
        if stats.count == 0:
            # No data yet
            bar_str = " " * BAR_WIDTH
            win.addstr(y, x, bar_str)
            return

        # Calculate positions
        min_pos = self.value_to_bar_position(stats.min_val, scale_min, scale_max)
        max_pos = self.value_to_bar_position(stats.max_val, scale_min, scale_max)
        avg_pos = self.value_to_bar_position(stats.avg, scale_min, scale_max)
        last_pos = self.value_to_bar_position(stats.last_val, scale_min, scale_max)

        # Determine fill range
        if fill_from_zero:
            fill_start = 0
            fill_end = last_pos
        else:
            fill_start = min_pos
            fill_end = max_pos

        # Build the bar character by character
        for i in range(BAR_WIDTH):
            char = ' '
            attr = curses.color_pair(COLOR_NORMAL)  # Default

            # Check if within the fill range
            if fill_start <= i <= fill_end:
                # Background fill
                char = '█'
                attr = curses.color_pair(color_bar)

                # Check for indicators
                if i == avg_pos and i == last_pos:
                    char = '*'
                    if flash_last:
                        attr = curses.color_pair(color_indicator_flash) | curses.A_BOLD
                    else:
                        attr = curses.color_pair(color_indicator) | curses.A_BOLD
                elif i == last_pos:
                    char = '*'
                    if flash_last:
                        attr = curses.color_pair(color_indicator_flash) | curses.A_BOLD
                    else:
                        attr = curses.color_pair(color_indicator) | curses.A_BOLD
                elif i == avg_pos:
                    char = '|'
                    attr = curses.color_pair(color_indicator) | curses.A_BOLD

            try:
                win.addch(y, x + i, char, attr)
            except curses.error:
                pass  # Ignore errors at screen edge

    def render_node_row(self, win, row: int, node: RelayNodeStats,
                        total_relayed: int, is_selected: bool, max_width: int) -> None:
        """Render two rows for a single relay node (rxSnr and rxRssi).

        Row 1: Hex ID of relay byte, packet count, rxSnr stats, rxSnr bar
        Row 2: Node name (if unique match), percentage, rxRssi stats, rxRssi bar

        Note: Selection highlight only applies to name/cnt/stats columns, not the bar.
        """
        # Calculate column positions
        name_col = 1
        range_col = 13
        cnt_col = 21
        known_col = 27
        stats_col = 34
        bar_col = 52
        last_col = bar_col + BAR_WIDTH + 1

        # Row 1: Hex ID with match count, packet count, rxSnr stats, rxSnr bar
        attr = curses.color_pair(COLOR_SELECTED) if is_selected else curses.color_pair(COLOR_NORMAL)

        # Selection indicator, hex ID, and match count
        prefix = ">" if is_selected else " "
        match_count = len(self.stats.find_matching_nodes(node.relay_node_byte))
        hex_str = f"{prefix}{node.hex_id}[{match_count}]"
        hex_str = f"{hex_str:<11}"
        try:
            win.addstr(row, name_col, hex_str, attr)
        except curses.error:
            pass

        # Range (row 1: distance) and row 2 (altitude) from get_node_location_info when possible
        distance, height = None, None
        current_pos = self.stats.get_local_node_position()
        if match_count == 1 and current_pos:
            node_num = next(
                (n for n in self.stats._nodes_by_num
                 if self.stats.get_last_byte_of_node_num(n) == node.relay_node_byte),
                None
            )
            if node_num is not None:
                loc = self.stats.get_node_location_info(node_num, current_pos)
                distance = loc.get("distance")
                height = loc.get("altitude")

        if distance is not None:
            range_str = f"{distance:>6.1f}km"
        else:
            range_str = "   N/A"
        try:
            win.addstr(row, range_col, range_str, attr)
        except curses.error:
            pass

        # Packet count
        cnt_str = f"{node.packet_count:>5}"
        try:
            win.addstr(row, cnt_col, cnt_str, attr)
        except curses.error:
            pass

        #  (distinct "from" nodes retransmitted via this relay)
        known_str = f"{node.known_nodes_count:>5}"
        try:
            win.addstr(row, known_col, known_str, attr)
        except curses.error:
            pass

        # rxSnr min/avg/max
        if node.snr.count > 0:
            snr_str = f"{node.snr.min_val:>4.0f}/{node.snr.avg:>4.1f}/{node.snr.max_val:>4.0f}"
        else:
            snr_str = "  --/  --/  --"
        try:
            win.addstr(row, stats_col, snr_str, attr)
        except curses.error:
            pass

        # rxSnr bar (no selection highlight, green color scheme)
        if bar_col + BAR_WIDTH < max_width:
            if self.vis_mode == VIS_MODE_SIMPLE:
                self.render_bar_simple(win, row, bar_col, node.snr,
                                       SNR_SCALE_MIN, SNR_SCALE_MAX, COLOR_SNR_BAR)
            else:
                self.render_bar_complex(win, row, bar_col, node.snr,
                                        SNR_SCALE_MIN, SNR_SCALE_MAX,
                                        COLOR_SNR_BAR, COLOR_SNR_INDICATOR,
                                        COLOR_SNR_INDICATOR_FLASH, node.just_received)

        # Row 2: Node name (if unique), percentage, rxRssi stats, rxRssi bar
        row2 = row + 1

        # Node name (only shown if exactly one node matches the relay byte)
        name_display = node.node_name[:10] if node.node_name else ""
        try:
            win.addstr(row2, name_col, f" {name_display:<10}", attr)
        except curses.error:
            pass

        # Range row 2: height (m) from get_node_location_info
        if match_count == 1:
            if height is not None:
                height_str = f"{height:>5}m"
            else:
                height_str = "   N/A"
            try:
                win.addstr(row2, range_col, height_str, attr)
            except curses.error:
                pass
        else:
            try:
                win.addstr(row2, range_col, "      ", attr)
            except curses.error:
                pass

        # Percentage
        pct = (node.packet_count / total_relayed * 100) if total_relayed > 0 else 0
        pct_str = f"{pct:>4.1f}%"
        try:
            win.addstr(row2, cnt_col, pct_str, attr)
        except curses.error:
            pass

        # Row 2 under Known nodes: leave empty (single value shown on row 1)
        try:
            win.addstr(row2, known_col, "     ", attr)
        except curses.error:
            pass

        # rxRssi min/avg/max
        if node.rssi.count > 0:
            rssi_str = f"{node.rssi.min_val:>4.0f}/{node.rssi.avg:>4.1f}/{node.rssi.max_val:>4.0f}"
        else:
            rssi_str = "  --/  --/  --"
        try:
            win.addstr(row2, stats_col, rssi_str, attr)
        except curses.error:
            pass

        # rxRssi bar (no selection highlight, cyan color scheme)
        if bar_col + BAR_WIDTH < max_width:
            if self.vis_mode == VIS_MODE_SIMPLE:
                self.render_bar_simple(win, row2, bar_col, node.rssi,
                                       RSSI_SCALE_MIN, RSSI_SCALE_MAX, COLOR_RSSI_BAR)
            else:
                self.render_bar_complex(win, row2, bar_col, node.rssi,
                                        RSSI_SCALE_MIN, RSSI_SCALE_MAX,
                                        COLOR_RSSI_BAR, COLOR_RSSI_INDICATOR,
                                        COLOR_RSSI_INDICATOR_FLASH, node.just_received)

        # Last packet timestamp (row 1) and time since (row 2)
        if last_col < max_width - 8:
            if node.last_packet_time > 0:
                from datetime import datetime
                # Row 1: timestamp HH:MM:SS
                dt = datetime.fromtimestamp(node.last_packet_time)
                time_str = dt.strftime("%H:%M:%S")
                try:
                    win.addstr(row, last_col, time_str, attr)
                except curses.error:
                    pass

                # Row 2: time since last packet
                elapsed = time.time() - node.last_packet_time
                if elapsed < 60:
                    since_str = f"{int(elapsed):>3}s ago"
                elif elapsed < 3600:
                    mins = int(elapsed // 60)
                    secs = int(elapsed % 60)
                    since_str = f"{mins}m {secs:02d}s"
                else:
                    hours = int(elapsed // 3600)
                    mins = int((elapsed % 3600) // 60)
                    since_str = f"{hours}h {mins:02d}m"
                try:
                    win.addstr(row2, last_col, since_str, attr)
                except curses.error:
                    pass

    def render_header(self, win, max_width: int) -> None:
        """Render the header section."""
        total = self.stats.get_total_packets()
        total_relayed = self.stats.get_total_relayed_packets()
        paused = self.stats.is_paused()
        sort_name = SORT_MODES[self.stats.sort_mode][1]
        db_time = self.stats.get_db_load_time_str()
        db_cnt = self.stats.get_db_cnt()
        spinner = self.stats.get_spinner_char()

        # Title line
        title = " Meshtastic Relay Node Statistics "
        # Spinner placed 2 chars before "Total"
        status = f"{spinner} Total: {total} | Relayed: {total_relayed} | Sort: {sort_name}"
        if paused:
            status += " | PAUSED"

        try:
            win.addstr(0, 0, "=" * max_width, curses.color_pair(COLOR_HEADER))
            win.addstr(0, 2, title, curses.color_pair(COLOR_HEADER) | curses.A_BOLD)
            # DB load time after title
            db_info = f" DB({db_cnt}): {db_time} "
            win.addstr(0, len(title) + 4, db_info, curses.color_pair(COLOR_HEADER))
            win.addstr(0, max_width - len(status) - 2, status, curses.color_pair(COLOR_PAUSED if paused else COLOR_HEADER))

            # Local node info (one line, above column headers)
            self.render_my_info(win, 1, max_width)

            # Column headers
            win.addstr(2, 0, "-" * max_width, curses.color_pair(COLOR_NORMAL))
            win.addstr(3, 1, "Relay", curses.color_pair(COLOR_HEADER))
            win.addstr(3, 13, "Range", curses.color_pair(COLOR_HEADER))
            win.addstr(4, 13, "Alt", curses.color_pair(COLOR_HEADER))
            win.addstr(3, 21, "Cnt", curses.color_pair(COLOR_HEADER))
            win.addstr(3, 27, "Known", curses.color_pair(COLOR_HEADER))
            win.addstr(4, 27, "nodes", curses.color_pair(COLOR_HEADER))
            win.addstr(3, 34, "min/avg/max", curses.color_pair(COLOR_HEADER))

            # Bar scale headers
            bar_col = 52
            last_col = bar_col + BAR_WIDTH + 1
            snr_header = f"rxSnr: {SNR_SCALE_MIN} dB"
            snr_header_end = f"{SNR_SCALE_MAX:+d} dB"
            win.addstr(3, bar_col, snr_header, curses.color_pair(COLOR_HEADER))
            win.addstr(3, bar_col + BAR_WIDTH - len(snr_header_end), snr_header_end, curses.color_pair(COLOR_HEADER))
            # Last column header
            win.addstr(3, last_col, "Last", curses.color_pair(COLOR_HEADER))

            rssi_header = f"rxRssi: {RSSI_SCALE_MIN} dBm"
            rssi_header_end = f"{RSSI_SCALE_MAX} dBm"
            win.addstr(4, bar_col, rssi_header, curses.color_pair(COLOR_HEADER))
            win.addstr(4, bar_col + BAR_WIDTH - len(rssi_header_end), rssi_header_end, curses.color_pair(COLOR_HEADER))

            win.addstr(5, 0, "-" * max_width, curses.color_pair(COLOR_NORMAL))
        except curses.error:
            pass

    def render_footer(self, win, y: int, max_width: int) -> None:
        """Render the footer/help section."""
        vis_name = VIS_MODES[self.vis_mode][1]
        try:
            win.addstr(y, 0, "-" * max_width, curses.color_pair(COLOR_NORMAL))
            if self.replay_mode:
                help_str = f" [Up/Dn] Navigate | [Enter] Details | [S]ort | [M]ode:{vis_name} | [P]ause | [R]eset | [Q]uit | REPLAY "
            else:
                help_str = f" [Up/Dn] Navigate | [Enter] Details | [S]ort | [M]ode:{vis_name} | [D]B reload | [P]ause | [R]eset | [Q]uit "
            win.addstr(y + 1, 0, help_str, curses.color_pair(COLOR_STATUS_BAR))
            # Pad the rest of the line
            if len(help_str) < max_width:
                win.addstr(y + 1, len(help_str), " " * (max_width - len(help_str)), curses.color_pair(COLOR_STATUS_BAR))
        except curses.error:
            pass

    def render_position_oneline(self, node_num, loc_info: Dict, prefix: str) -> Optional[str]:
        lat, lon = loc_info.get("lat"), loc_info.get("lon"),
        dist, alt = loc_info.get("distance"), loc_info.get("altitude")
        dir = loc_info.get("direction")
        obfs_rad = loc_info.get("obfuscation_radius")
        src = loc_info.get("src")
        result = prefix
        if lat is not None and lon is not None:
            result = result + f"{lat:.6f}, {lon:.6f} "
            if dist is not None:
                if dir and dir != "un":
                    dir_str = f"/{dir}"
                else:
                    dir_str = ""
                delta = ""
                if obfs_rad is not None:
                    obfs_rad_km = obfs_rad / 1000.0
                    if obfs_rad_km >= 0.1:
                        delta = f"±{obfs_rad_km:.1f}"

                result = result + f"(Dist: {dist:.1f}{delta} km{dir_str}) "
        if alt is not None:
            result = result + f"(Alt: {alt}m) "
        if src is not None:
            ts = loc_info.get("timestamp")
            if ts is not None:
                elapsed = time.time() - ts
                if elapsed < 60:
                    age_str = "1m"
                elif elapsed < 5 * 60:
                    age_str = "5m"
                elif elapsed < 30 * 60:
                    age_str = "30m"
                elif elapsed < 3600:
                    age_str = "1h"
                elif elapsed < 12 * 3600:
                    age_str = "12h"
                elif elapsed < 24 * 3600:
                    age_str = "1d"
                elif elapsed < 7 * 24 * 3600:
                    age_str = "1w"
                elif elapsed < 365 * 24 * 3600:
                    age_str = "1y"
                else:
                    age_str = "??"
                result = result + f"(Src: {src}:{age_str}) "
            else:
                result = result + f"(Src: {src}) "
        if self.stats.meshview_url:
            meshview_link = f"{self.stats.meshview_url}/node/{node_num}"
            result = result + f"{meshview_link}"
        return result

    def build_detail_lines(self, node: RelayNodeStats) -> List[Tuple[str, int]]:
        """Build list of lines for detail view.

        Returns list of (text, color_pair) tuples.
        """
        lines = []
        CP_HEADER = curses.color_pair(COLOR_HEADER) | curses.A_BOLD
        CP_NORMAL = curses.color_pair(COLOR_NORMAL)
        CP_SECTION = curses.color_pair(COLOR_HEADER)
        matching_nodes = self.stats.find_matching_nodes(node.relay_node_byte)
        current_pos = self.stats.get_local_node_position()

        # Basic relay info
        lines.append((f"Relay Node Byte: {node.hex_id}", CP_HEADER))
        skipped_for_relay = [n for n in self.stats.skip_relays if (n & 0xFF) == node.relay_node_byte]
        if skipped_for_relay:
            skipped_str = ", ".join(f"!{n:08x}" for n in sorted(skipped_for_relay))
            lines.append((f"Explicitly skipped relay nodes: {skipped_str}", CP_NORMAL))
        lines.append((f"Total Packets Relayed: {node.packet_count}", CP_NORMAL))
        lines.append((f"Packets/hour: {node.packets_per_hour:.1f}", CP_NORMAL))
        lines.append(("", CP_NORMAL))

        # rxSnr details
        lines.append(("rxSnr (dB):\t\trxRssi (dBm):", CP_SECTION))
        if node.snr.count > 0 and node.rssi.count > 0:
            lines.append((f"  Min: {node.snr.min_val:.1f}\t\t{node.rssi.min_val:.1f}", CP_NORMAL))
            lines.append((f"  Avg: {node.snr.avg:.1f}\t\t{node.rssi.avg:.1f}", CP_NORMAL))
            lines.append((f"  Max: {node.snr.max_val:.1f}\t\t{node.rssi.max_val:.1f}", CP_NORMAL))
            lines.append((f"  Last: {node.snr.last_val:.1f}\t\t{node.rssi.last_val:.1f}", CP_NORMAL))
            lines.append((f"  Count: {node.snr.count}\t\t{node.rssi.count}", CP_NORMAL))
        else:
            lines.append(("  No data", CP_NORMAL))

        lines.append(("", CP_NORMAL))

        # Find all matching nodes in database
        lines.append(("-" * 60, CP_NORMAL))
        lines.append((f"Matching Nodes in Database: {len(matching_nodes)}", CP_HEADER))
        lines.append(("-" * 60, CP_NORMAL))

        if not matching_nodes:
            lines.append(("  No matching nodes found in database", CP_NORMAL))
            lines.append(("  (Node may not have sent its nodeinfo)", CP_NORMAL))
        else:
            for i, node_info in enumerate(matching_nodes):
                lines.append(("", CP_NORMAL))
                node_num = node_info.get("num")
                if node_num is None:
                    node_num = next((n for n, inf in self.stats._nodes_by_num.items() if inf is node_info), None)
                node_num = node_num if node_num is not None else 0
                lines.append((f"[{i+1}] Node !{node_num:08x}", CP_SECTION))

                role = None
                if "user" in node_info:
                    user = node_info["user"]
                    if "longName" in user:
                        lines.append((f"    Long Name: {user['longName']}", CP_NORMAL))
                    if "shortName" in user:
                        lines.append((f"    Short Name: {user['shortName']}", CP_NORMAL))
                    if "hwModel" in user:
                        lines.append((f"    Hardware: {user['hwModel']}", CP_NORMAL))
                    role = user.get("role") or user.get("deviceRole")
                    if role is None:
                        # Default role name is CLIENT
                        role = 'CLIENT'
                    lines.append((f"    Role: {role}", CP_NORMAL))

                # Distance, altitude and bearing from get_node_location_info when we have current_pos
                loc = self.stats.get_node_location_info(node_num, current_pos)
                dist = loc.get("distance")
                alt = loc.get("altitude")
                bearing = loc.get("direction")
                pos_line = self.render_position_oneline(node_num, loc, "    Position: ")
                lines.append((pos_line, CP_NORMAL))

                if loc.get("lon") and loc.get("lat"):
                    lat = loc.get("lat")
                    lon = loc.get("lon")
                    google_url = f"https://maps.google.com/?q={lat},{lon}"
                    lines.append((f"    Google Maps: {google_url}", CP_NORMAL))
                    osm_url = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15"
                    lines.append((f"    OpenStreetMap: {osm_url}", CP_NORMAL))

                if "snr" in node_info:
                    lines.append((f"    Last SNR in DB: {node_info['snr']:.1f} dB", CP_NORMAL))

                if "lastHeard" in node_info:
                    import datetime
                    ts = node_info["lastHeard"]
                    dt = datetime.datetime.fromtimestamp(ts)
                    lines.append((f"    Last Heard in DB: {dt.strftime('%Y-%m-%d %H:%M:%S')}", CP_NORMAL))

                # Firmware from node database
                fw = node_info.get("firmwareVersion") or node_info.get("firmware_version")
                if fw is not None:
                    lines.append((f"    Firmware: {fw}", CP_NORMAL))

                # Uptime, restarts and telemetry from received TELEMETRY_APP
                trec = self.stats._node_telemetry.get(node_num)
                if trec is not None:
                    if trec.last_uptime_seconds is not None:
                        secs = trec.last_uptime_seconds
                        d, r = divmod(secs, 86400)
                        h, r = divmod(r, 3600)
                        m, _ = divmod(r, 60)
                        uptime_str = f"{d}d {h}h {m}m"
                        lines.append((f"    Uptime: {uptime_str}, observed restarts: {trec.observed_restart_count} times", CP_NORMAL))
                    for k, hist in sorted(trec.history_metrics.items()):
                        if hist.count > 0:
                            lines.append((f"     {k}: {hist.last_val:.2f}", CP_NORMAL))

        # Known remote nodes (distinct "from" nodes), one line each, ordered by packet count descending
        lines.append(("", CP_NORMAL))
        lines.append(("-" * 60, CP_NORMAL))
        lines.append((f"Known Remote Nodes: {len(node.from_node_stats)}", CP_HEADER))
        lines.append(("-" * 60, CP_NORMAL))

        if not node.from_node_stats:
            lines.append(("  No remote nodes recorded yet", CP_NORMAL))
        else:
            # Column headers - must match data format below
            header = f"  {'Node ID':<10} {'Name':<8} {'Pkts':>4} {'Hops':>4} {'Left':>4} {'Position and GEO-info':>21}"
            lines.append((header, CP_HEADER))
            lines.append(("-" * 60, CP_NORMAL))
            # Sort by packet count descending
            sorted_remotes = sorted(
                node.from_node_stats.items(),
                key=lambda x: x[1].packet_count,
                reverse=True
            )
            for node_num, remote_stats in sorted_remotes:
                node_info = self.stats._nodes_by_num.get(node_num)
                short_name = ""
                if node_info and "user" in node_info:
                    short_name = (node_info["user"].get("shortName", "") or "")[:8]
                loc = self.stats.get_node_location_info(node_num, current_pos)
                pos_line = self.render_position_oneline(node_num, loc, "")
                # Hop statistics
                avg_hops = remote_stats.avg_hops_made
                avg_left = remote_stats.avg_hops_left
                hops_str = f"{avg_hops:3.1f}" if avg_hops is not None else "N/A"
                left_str = f"{avg_left:3.1f}" if avg_left is not None else "N/A"
                line = f"  !{node_num:08x} {short_name:8} {remote_stats.packet_count:>4} {hops_str:>4} {left_str:>4} {pos_line}"
                lines.append((line, CP_NORMAL))

        return lines

    def render_detail_view(self, win, node: RelayNodeStats, max_height: int, max_width: int) -> None:
        """Render detailed view for a selected relay node with scrolling."""
        try:
            win.clear()

            # Header
            win.addstr(0, 0, "=" * max_width, curses.color_pair(COLOR_HEADER))
            title = f" Relay Node Details: {node.hex_id} "
            if node.node_name:
                title += f"({node.node_name}) "
            win.addstr(0, 2, title, curses.color_pair(COLOR_HEADER) | curses.A_BOLD)
            # DB load time on the same line
            db_info = f" DB({self.stats.get_db_cnt()}): {self.stats.get_db_load_time_str()} "
            win.addstr(0, len(title) + 4, db_info, curses.color_pair(COLOR_HEADER))
            win.addstr(1, 0, "-" * max_width, curses.color_pair(COLOR_NORMAL))

            # Use cached lines if available for same node, otherwise rebuild
            if (self._detail_lines_cache is not None and
                    self._detail_cache_node_byte == node.relay_node_byte):
                lines = self._detail_lines_cache
            else:
                lines = self.build_detail_lines(node)
                self._detail_lines_cache = lines
                self._detail_cache_node_byte = node.relay_node_byte

            # Calculate visible area
            content_start_row = 2
            footer_rows = 2
            visible_rows = max_height - content_start_row - footer_rows

            # Adjust scroll offset
            total_lines = len(lines)
            max_scroll = max(0, total_lines - visible_rows)
            self.detail_scroll_offset = min(self.detail_scroll_offset, max_scroll)
            self.detail_scroll_offset = max(0, self.detail_scroll_offset)

            # Render visible lines
            for i in range(visible_rows):
                line_idx = self.detail_scroll_offset + i
                if line_idx >= total_lines:
                    break
                text, attr = lines[line_idx]
                # Truncate if too long
                if len(text) > max_width - 4:
                    text = text[:max_width - 7] + "..."
                try:
                    win.addstr(content_start_row + i, 2, text, attr)
                except curses.error:
                    pass

            # Scroll indicator
            if total_lines > visible_rows:
                scroll_info = f" [{self.detail_scroll_offset + 1}-{min(self.detail_scroll_offset + visible_rows, total_lines)}/{total_lines}] "
                try:
                    win.addstr(0, max_width - len(scroll_info) - 2, scroll_info, curses.color_pair(COLOR_HEADER))
                except curses.error:
                    pass

            # Footer
            footer_row = max_height - 2
            win.addstr(footer_row, 0, "-" * max_width, curses.color_pair(COLOR_NORMAL))
            help_str = " [Up/Dn] Scroll | [1]-[9],[0] Skip | [C]lear skipped | [Enter/Esc] Return "
            win.addstr(footer_row + 1, 0, help_str, curses.color_pair(COLOR_STATUS_BAR))
            if len(help_str) < max_width:
                win.addstr(footer_row + 1, len(help_str), " " * (max_width - len(help_str)), curses.color_pair(COLOR_STATUS_BAR))

        except curses.error:
            pass

    def render(self) -> None:
        """Main render function."""
        if self.stdscr is None:
            return

        self.stdscr.clear()
        max_height, max_width = self.stdscr.getmaxyx()

        nodes = self.stats.get_sorted_nodes()
        total_relayed = self.stats.get_total_relayed_packets()

        if self.show_details and nodes and 0 <= self.selected_index < len(nodes):
            self.render_detail_view(self.stdscr, nodes[self.selected_index], max_height, max_width)
        else:
            # Render header (title + local node line + column headers = 6 rows)
            self.render_header(self.stdscr, max_width)

            # Calculate available space for nodes (2 rows per node)
            header_rows = 6
            footer_rows = 2
            available_rows = max_height - header_rows - footer_rows
            max_visible_nodes = available_rows // 2

            # Adjust scroll offset
            if self.selected_index < self.scroll_offset:
                self.scroll_offset = self.selected_index
            elif self.selected_index >= self.scroll_offset + max_visible_nodes:
                self.scroll_offset = self.selected_index - max_visible_nodes + 1

            # Render visible nodes
            visible_nodes = nodes[self.scroll_offset:self.scroll_offset + max_visible_nodes]
            for i, node in enumerate(visible_nodes):
                row = header_rows + i * 2
                is_selected = (self.scroll_offset + i) == self.selected_index
                self.render_node_row(self.stdscr, row, node, total_relayed, is_selected, max_width)

            # Render footer
            self.render_footer(self.stdscr, max_height - 2, max_width)

        self.stdscr.refresh()

    def _confirm_popup(self, message: str, subline: Optional[str] = None) -> bool:
        """Show a confirmation popup with message (and optional subline). Returns True if user presses y/Y."""
        popup = None
        try:
            height, width = self.stdscr.getmaxyx()
            w = min(58, max(40, len(message) + 4))
            if subline:
                w = max(w, min(58, len(subline) + 4))
            h = 6 if subline else 5
            y = max(0, (height - h) // 2)
            x = max(0, (width - w) // 2)
            popup = curses.newwin(h, w, y, x)
            popup.keypad(True)
            msg_trim = message[: w - 4] if len(message) > w - 4 else message
            popup.addstr(1, max(0, (w - len(msg_trim)) // 2), msg_trim, curses.color_pair(COLOR_HEADER))
            if subline:
                sub_trim = subline[: w - 4] if len(subline) > w - 4 else subline
                popup.addstr(2, max(0, (w - len(sub_trim)) // 2), sub_trim, curses.color_pair(COLOR_NORMAL))
            prompt_row = 4 if subline else 3
            popup.addstr(prompt_row, max(0, (w - 15) // 2), "[y] Yes  [n] No", curses.color_pair(COLOR_NORMAL))
            popup.border()
            popup.refresh()
            self.stdscr.nodelay(False)
            self.stdscr.timeout(-1)
            key = self.stdscr.getch()
            confirmed = key in (ord('y'), ord('Y'))
        finally:
            self.stdscr.nodelay(True)
            self.stdscr.timeout(250)
            if popup is not None:
                try:
                    popup.clear()
                    del popup
                except Exception:
                    pass
        return confirmed

    def handle_input(self, key: int) -> None:
        """Handle keyboard input."""
        nodes = self.stats.get_sorted_nodes()
        num_nodes = len(nodes)

        if self.show_details:
            # In detail view: scroll with Up/Down, return with Enter/Escape, [1]-[9],[0] to skip node
            if key == curses.KEY_UP:
                self.detail_scroll_offset = max(0, self.detail_scroll_offset - 1)
            elif key == curses.KEY_DOWN:
                self.detail_scroll_offset += 1  # Will be clamped in render
            elif key == curses.KEY_PPAGE:  # Page Up
                self.detail_scroll_offset = max(0, self.detail_scroll_offset - 10)
            elif key == curses.KEY_NPAGE:  # Page Down
                self.detail_scroll_offset += 10  # Will be clamped in render
            elif key in (curses.KEY_ENTER, 10, 13, 27):
                self.show_details = False
                self.detail_scroll_offset = 0
                # Invalidate cache when leaving detail view
                self._detail_lines_cache = None
                self._detail_cache_node_byte = None
            elif num_nodes > 0 and 0 <= self.selected_index < num_nodes:
                # Keys 1-9 -> matching node index 0-8, key 0 -> index 9
                match_index = -1
                if ord('1') <= key <= ord('9'):
                    match_index = key - ord('1')
                elif key == ord('0'):
                    match_index = 9
                if match_index >= 0:
                    node = nodes[self.selected_index]
                    matching_nodes = self.stats.find_matching_nodes(node.relay_node_byte)
                    if match_index < len(matching_nodes):
                        node_info = matching_nodes[match_index]
                        node_num = node_info.get("num")
                        if node_num is None:
                            node_num = next((n for n, inf in self.stats._nodes_by_num.items() if inf is node_info), None)
                        if node_num is not None and self._confirm_popup(f"Skip node !{node_num:08x}? (y/n)"):
                            self.stats.add_skipped_relay_node(node_num, "user")
                            self._detail_lines_cache = None
                            self._detail_cache_node_byte = None
                elif key in (ord('c'), ord('C')):
                    # Clear all skipped routers whose last byte matches this relay
                    node = nodes[self.selected_index]
                    relay_byte = node.relay_node_byte
                    to_remove = [k for k in self.stats.skip_relays if self.stats.skip_relays[k] == "user" and (k & 0xFF) == relay_byte]
                    if to_remove:
                        msg = f"Clear all skipped routers for this relay (!..{relay_byte:02x})? (y/n)"
                        sub = f"({len(to_remove)} node(s))"
                        if self._confirm_popup(msg, sub):
                            for n in to_remove:
                                self.stats.rem_skipped_relay_node(n)
                            self._detail_lines_cache = None
                            self._detail_cache_node_byte = None
        else:
            # In main view: Escape does NOT exit, only Q quits
            if key == curses.KEY_UP and num_nodes > 0:
                self.selected_index = max(0, self.selected_index - 1)
            elif key == curses.KEY_DOWN and num_nodes > 0:
                self.selected_index = min(num_nodes - 1, self.selected_index + 1)
            elif key in (curses.KEY_ENTER, 10, 13) and num_nodes > 0:
                self.show_details = True
                self.detail_scroll_offset = 0
                # Invalidate cache to rebuild lines for the selected node
                self._detail_lines_cache = None
                self._detail_cache_node_byte = None
            elif key in (ord('p'), ord('P')):
                self.stats.toggle_pause()
            elif key in (ord('r'), ord('R')):
                self.stats.reset()
                self.selected_index = 0
                self.scroll_offset = 0
            elif key in (ord('s'), ord('S')):
                self.stats.cycle_sort_mode()
            elif key in (ord('m'), ord('M')):
                self.vis_mode = (self.vis_mode + 1) % len(VIS_MODES)
            elif key in (ord('d'), ord('D')) and not self.replay_mode:
                self.stats.reload_node_database()
            elif key in (ord('q'), ord('Q')):
                self.running = False

    def run(self, stdscr) -> None:
        """Main TUI loop."""
        self.stdscr = stdscr

        # Setup curses
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(True)  # Non-blocking input
        stdscr.timeout(250)  # Refresh every 250ms

        self.init_colors()

        while self.running:
            self.render()

            try:
                key = stdscr.getch()
                if key != -1:
                    self.handle_input(key)
            except curses.error:
                pass


class PacketWriter:
    """Writes received packets to a binary file using pickle.

    File format:
    - First record: ('__nodedb__', nodes_by_num_dict, db_load_time, local_position)
    - Subsequent records: (timestamp, packet_dict)
    """

    def __init__(self, filename: str):
        self.filename = filename
        self.file = None
        self.lock = threading.Lock()
        self.packet_count = 0

    def open(self, nodes_by_num: Optional[Dict] = None, db_load_time: Optional[float] = None,
             local_position: Optional[Tuple[float, float]] = None) -> None:
        """Open the file for writing and save node database header.

        The file is opened in binary write mode ('wb'); any existing file
        with the same path will be overwritten.
        """
        self.file = open(self.filename, 'wb')
        self.packet_count = 0
        header = ('__nodedb__', nodes_by_num or {}, db_load_time, local_position)
        pickle.dump(header, self.file, protocol=pickle.HIGHEST_PROTOCOL)
        self.file.flush()

    def write_packet(self, packet: Dict) -> None:
        """Write a packet to the file with timestamp."""
        if self.file is None:
            return
        with self.lock:
            try:
                record = (time.time(), dict(packet))
                pickle.dump(record, self.file, protocol=pickle.HIGHEST_PROTOCOL)
                self.file.flush()
                self.packet_count += 1
            except Exception as e:
                print(f"Error writing packet: {e}", file=sys.stderr)

    def close(self) -> None:
        """Close the file."""
        if self.file is not None:
            with self.lock:
                self.file.close()
                self.file = None
            print(f"Saved {self.packet_count} packets to {self.filename}")


class PacketReplayer:
    """Replays packets from a binary pickle file."""

    def __init__(self, filename: str, stats: 'StatsCollector', speed: float = 1.0):
        self.filename = filename
        self.stats = stats
        self.speed = speed
        self.packets: List[Tuple[float, Dict]] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.current_index = 0
        self.paused = False
        self.lock = threading.Lock()
        self.nodes_by_num: Dict = {}
        self.db_load_time: Optional[float] = None
        self.local_position: Optional[Tuple[float, float]] = None

    def load(self) -> bool:
        """Load node database and packets from file."""
        try:
            with open(self.filename, 'rb') as f:
                first_record = True
                while True:
                    try:
                        record = pickle.load(f)
                        if first_record:
                            first_record = False
                            if (isinstance(record, tuple) and len(record) >= 3
                                    and record[0] == '__nodedb__'):
                                self.nodes_by_num = record[1] or {}
                                self.db_load_time = record[2]
                                # Load local position if available (new format)
                                if len(record) >= 4:
                                    self.local_position = record[3]
                                print(f"Loaded node database with {len(self.nodes_by_num)} nodes")
                                if self.local_position:
                                    print(f"Loaded local position: {self.local_position}")
                                continue
                            elif isinstance(record, tuple) and len(record) == 2:
                                self.packets.append(record)
                        elif isinstance(record, tuple) and len(record) == 2:
                            self.packets.append(record)
                        else:
                            print(f"Warning: Skipping malformed record in {self.filename}", file=sys.stderr)
                    except EOFError:
                        break
            print(f"Loaded {len(self.packets)} packets from {self.filename}")
            return True
        except FileNotFoundError:
            print(f"Error: File not found: {self.filename}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"Error loading {self.filename}: {e}", file=sys.stderr)
            return False

    def start(self) -> None:
        """Start replaying packets in a background thread."""
        if not self.packets:
            return
        self.running = True
        self.current_index = 0
        self.thread = threading.Thread(target=self._replay_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Stop replaying."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
            self.thread = None

    def toggle_pause(self) -> None:
        """Toggle pause state."""
        with self.lock:
            self.paused = not self.paused

    def is_paused(self) -> bool:
        """Check if replay is paused."""
        with self.lock:
            return self.paused

    def get_progress(self) -> Tuple[int, int]:
        """Get current replay progress (current_index, total)."""
        with self.lock:
            return (self.current_index, len(self.packets))

    def _replay_loop(self) -> None:
        """Main replay loop running in background thread."""
        prev_timestamp = None

        while self.running and self.current_index < len(self.packets):
            with self.lock:
                if self.paused:
                    time.sleep(0.1)
                    continue
                timestamp, packet = self.packets[self.current_index]
                self.current_index += 1

            if prev_timestamp is not None and self.speed != float('inf'):
                delay = (timestamp - prev_timestamp) / self.speed
                if delay > 0:
                    delay = min(delay, 5.0)
                    time.sleep(delay)
            prev_timestamp = timestamp

            self.stats.on_receive(packet, None)

        self.running = False


def connect_serial(device: str):
    """Connect to Meshtastic device via serial port."""
    print(f"Connecting to serial device: {device}")
    interface = meshtastic.serial_interface.SerialInterface(devPath=device)
    return interface


def connect_ble(address: str):
    """Connect to Meshtastic device via Bluetooth."""
    print(f"Connecting to BLE device: {address}")
    interface = meshtastic.ble_interface.BLEInterface(address=address)
    return interface


def wait_for_node_db(interface, timeout: int = 30) -> None:
    """Wait for node database to be loaded."""
    print("Waiting for node database to load...")
    try:
        interface.waitForConfig()
    except Exception as e:
        print(f"Warning: Config wait issue: {e}")

    # Report loaded nodes
    if interface.nodesByNum:
        print(f"Loaded {len(interface.nodesByNum)} nodes from database")
        for node_num, node_info in interface.nodesByNum.items():
            name = "?"
            if "user" in node_info:
                name = node_info["user"].get("shortName", node_info["user"].get("longName", "?"))
            print(f"  - Node !{node_num:08x}: {name}")
    else:
        print("No nodes in database yet")


def parse_meshtastic_node_id(value: str) -> int:
    """Parse Meshtastic node id from string (!xxxxxxxx or xxxxxxxx, hex) to integer."""
    s = value.strip()
    if s.startswith("!"):
        s = s[1:]
    return int(s, 16)


def main():
    parser = argparse.ArgumentParser(
        description="Meshtastic Relay Node Statistics TUI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --serial /dev/ttyACM0
  %(prog)s --ble "Meshtastic_abcd"
  %(prog)s --ble "AA:BB:CC:DD:EE:FF"
  %(prog)s --serial /dev/ttyACM0 --meshview https://meshview.meshtastic.es
  %(prog)s --serial /dev/ttyACM0 -w packets.dat     # Record packets to file
  %(prog)s -r packets.dat                           # Replay from file
  %(prog)s -r packets.dat --speed 5                 # Replay at 5x speed
        """
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--serial", "-s",
        metavar="DEVICE",
        help="Serial port device path (e.g., /dev/ttyACM0, /dev/ttyUSB0)"
    )
    group.add_argument(
        "--ble", "-b",
        metavar="ADDRESS",
        help="Bluetooth device address or name"
    )

    parser.add_argument(
        "--meshview", "-m",
        metavar="URL",
        help="Meshview base URL (e.g., https://meshview.meshtastic.es)"
    )

    parser.add_argument(
        "-w", "--write",
        metavar="FILE",
        help="Write received packets to FILE (overwrites if exists; use with --serial or --ble)"
    )

    parser.add_argument(
        "-r", "--replay",
        metavar="FILE",
        help="Replay packets from .dat file (cannot be combined with --serial or --ble)"
    )

    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        metavar="FACTOR",
        help="Replay speed multiplier (default: 1.0, use 0 for instant replay)"
    )

    parser.add_argument(
        "--skip-relay",
        action="append",
        default=None,
        metavar="NODE_ID",
        type=parse_meshtastic_node_id,
        help="Skip node from relay list (Meshtastic id in hex: !xxxxxxxx or xxxxxxxx); can be repeated"
    )

    args = parser.parse_args()

    if args.replay and (args.serial or args.ble):
        parser.error("--replay (-r) cannot be combined with --serial or --ble")

    if not args.serial and not args.ble and not args.replay:
        parser.error("one of --serial, --ble, or --replay is required")

    # Create statistics collector
    stats = StatsCollector(meshview_url=args.meshview)

    if args.skip_relay:
        for node_num in args.skip_relay:
            stats.add_skipped_relay_node(node_num)

    # Packet writer (if -w specified)
    packet_writer: Optional[PacketWriter] = None
    if args.write:
        packet_writer = PacketWriter(args.write)

    # Packet replayer (if -r specified)
    replayer: Optional[PacketReplayer] = None
    if args.replay:
        replayer = PacketReplayer(args.replay, stats, speed=args.speed if args.speed > 0 else float('inf'))
        if not replayer.load():
            sys.exit(1)

    # Custom receive handler that also writes to file
    def on_receive_with_write(packet, interface):
        stats.on_receive(packet, interface)
        if packet_writer:
            packet_writer.write_packet(packet)

    # Subscribe to meshtastic receive events (only if using real device)
    if not args.replay:
        pub.subscribe(on_receive_with_write, "meshtastic.receive")

    # Connect to device or start replay
    interface = None
    replay_mode = bool(args.replay)
    try:
        if args.replay:
            # Replay mode - no device connection
            print("Starting replay mode...")
            # Load node database from saved file
            stats.load_node_database(replayer.nodes_by_num, replayer.db_load_time,
                                     replayer.local_position)
            replayer.start()
        else:
            # Real device connection
            if args.serial:
                interface = connect_serial(args.serial)
            else:
                interface = connect_ble(args.ble)

            stats.set_interface(interface)

            # Wait for node database to be fully loaded, then take snapshot for thread-safe lookups
            wait_for_node_db(interface)
            stats.reload_node_database()

            if packet_writer:
                # Save node database along with packets, including local position
                local_pos = stats.get_local_node_position()
                packet_writer.open(stats._nodes_by_num, stats.db_load_time, local_pos)

        print("Starting TUI...")
        time.sleep(0.5)

        # Run TUI
        tui = MeshStatsTUI(stats, replay_mode=replay_mode)
        curses.wrapper(tui.run)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if replayer:
            replayer.stop()
        if packet_writer:
            packet_writer.close()
        if interface:
            try:
                interface.close()
            except:
                pass
        print("Disconnected.")


if __name__ == "__main__":
    main()
