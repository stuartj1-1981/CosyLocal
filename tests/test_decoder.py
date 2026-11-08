"""Decoder tests. Run from the repository root:

    python3 -m unittest discover -s tests -v

No MQTT broker and no paho-mqtt package are needed.
"""
import importlib.util
import json
import logging
import unittest
from pathlib import Path

DECODER = Path(__file__).resolve().parents[1] / "qsh-modbus-sniffer/rootfs/opt/qsh/cosy6_decoder.py"

logging.disable(logging.WARNING)  # hide the "paho-mqtt not installed" warning
_spec = importlib.util.spec_from_file_location("cosy6_decoder", DECODER)
dec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dec)


class FakeClient:
    def __init__(self):
        self.published = {}

    def publish(self, topic, payload, retain=False, qos=0):
        self.published[topic] = payload


def discovery_payload(reg):
    pub = dec.MQTTPublisher.__new__(dec.MQTTPublisher)
    pub.config = {"app_version": "test"}
    pub.client = FakeClient()
    pub.connected = True
    pub.discovery_sent = set()
    pub.base_topic = "Cosy HP"
    pub._send_discovery(reg)
    return json.loads(pub.client.published[f"homeassistant/sensor/qsh_modbus/reg_{reg}/config"])


class TestSignedDecode(unittest.TestCase):
    # Acceptance check D1
    def test_r36_oat_below_zero(self):
        self.assertEqual(dec.decode_register(36, 65516), -2.0)

    def test_r34_start_transient(self):
        self.assertEqual(dec.decode_register(34, 64648), -88.8)

    def test_r35_start_transient(self):
        self.assertEqual(dec.decode_register(35, 64648), -88.8)

    def test_r34_r35_never_above_1000_for_any_raw(self):
        # Acceptance check D2, decoder side: int16 × 0.1 is inside ±3276.8,
        # and the old wrap values 6464.4-6525.0 cannot occur.
        for reg in (34, 35):
            for raw in range(64644, 65251):
                self.assertLess(dec.decode_register(reg, raw), 0)

    def test_signed_temperatures(self):
        for reg in (37, 39, 41, 43, 54, 55, 56, 57, 58):
            self.assertIn(reg, dec.SIGNED_REGISTERS, reg)
        self.assertEqual(dec.decode_register(58, 65416), -12.0)
        self.assertEqual(dec.decode_register(56, 65535), -0.1)
        self.assertEqual(dec.decode_register(56, 65536 - 10230), -1023.0)

    def test_positive_values_unchanged(self):
        self.assertEqual(dec.decode_register(36, 52), 5.2)
        self.assertEqual(dec.decode_register(50, 389), 3.89)
        self.assertEqual(dec.decode_register(20, 1700), 1700)

    def test_unsigned_counter_stays_unsigned(self):
        self.assertNotIn(67, dec.SIGNED_REGISTERS)
        self.assertEqual(dec.decode_register(67, 40000), 40000)


class TestDiscovery(unittest.TestCase):
    def test_unique_id_unchanged(self):
        for reg in dec.REGISTER_NAMES:
            if reg in (210,):
                continue
            self.assertEqual(discovery_payload(reg)["unique_id"], f"qsh_modbus_reg_{reg}")

    def test_names(self):
        expected = {
            19: "Unknown 19", 20: "Electrical Power Input", 24: "Unknown 24",
            25: "DC Link Voltage (candidate)", 28: "Unknown 28",
            29: "Compressor Area Temp 1", 30: "Compressor Area Temp 2",
            32: "Main EEV Position", 34: "Defrost Valve Position", 35: "Unknown 35",
            37: "Unknown Temp 37", 39: "Unknown Temp 39", 41: "Compressor Sump Temp",
            45: "Discharge Gas Temp", 48: "Discharge Pressure (gauge)",
            50: "Suction Pressure (gauge)", 51: "Circulation Pump Speed",
            52: "Water Flow Target", 53: "Compressor Demand", 54: "Unknown Temp 54",
            55: "Suction Superheat (calc)", 56: "Saturated Condensing Temp (calc)",
            57: "Saturated Evaporating Temp (calc)", 58: "Defrost Te Threshold",
            60: "Unknown 60", 62: "Fan Speed Command", 63: "Compressor Speed",
            66: "HP State Sub-step", 67: "Sub-step Timer", 77: "Standby Pump Run Flag",
        }
        for reg, name in expected.items():
            self.assertEqual(discovery_payload(reg)["name"], f"Cosy {name}", reg)

    def test_state_class(self):
        measured = (19, 20, 24, 25, 28, 29, 30, 32, 34, 35, 37, 39, 41, 45, 50,
                    51, 52, 53, 54, 55, 56, 57, 62, 63, 66, 67, 77)
        for reg in measured:
            self.assertEqual(discovery_payload(reg).get("state_class"), "measurement", reg)
        for reg in (58, 60):
            self.assertNotIn("state_class", discovery_payload(reg), reg)

    def test_units_and_classes(self):
        p = discovery_payload(55)
        self.assertEqual(p["unit_of_measurement"], "K")
        self.assertNotIn("device_class", p)
        p = discovery_payload(50)
        self.assertEqual((p["unit_of_measurement"], p["device_class"]), ("bar", "pressure"))
        p = discovery_payload(35)
        self.assertNotIn("unit_of_measurement", p)
        p = discovery_payload(58)
        self.assertEqual((p["unit_of_measurement"], p["device_class"]), ("°C", "temperature"))


if __name__ == "__main__":
    unittest.main()
