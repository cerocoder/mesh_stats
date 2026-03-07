# Heltec T114 → Ubuntu 22.04 via Bluetooth

Short checklist for connecting a Meshtastic Heltec Mesh Node T114 over BLE on Ubuntu 22.04.

## 1. Bluetooth stack

```bash
sudo apt install bluez
sudo systemctl start bluetooth
sudo systemctl enable bluetooth
rfkill unblock bluetooth
```

## 2. Remove old pairing (important)

If the T114 was paired before, **remove it in the desktop Bluetooth settings**:

- Open **Settings → Bluetooth**
- Find the Meshtastic/Heltec device and **remove** (forget) it

Then power the T114 off and on so it advertises again.

## 3. Pair with bluetoothctl (optional)

```bash
bluetoothctl
```

In `bluetoothctl`:

```
power on
scan on
```

Wait until the T114 appears (e.g. `Meshtastic_xxxx`). Then:

```
pair XX:XX:XX:XX:XX:XX
connect XX:XX:XX:XX:XX:XX
trust XX:XX:XX:XX:XX:XX
scan off
quit
```

## 4. Connect with Python (mesh_stats or meshtastic-cli)

By device name:

```bash
python3 mesh_stats.py --ble "Meshtastic_xxxx"
```

Or by MAC address:

```bash
python3 mesh_stats.py --ble "AA:BB:CC:DD:EE:FF"
```

Only one client can use the device at a time (close Web Client / other apps if connection fails).
