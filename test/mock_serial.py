"""Mock for A02YYUW serial output using a pseudo-tty (PTY).

This module creates a PTY pair and continuously writes valid A02YYUW
4-byte frames to the PTY master. The slave device can be used as a
serial device by other programs (for example the main program in this
repo). By default the module will attempt to symlink the PTY slave to
`/dev/ttyUSB0` so the code in `main.py` can open the default port.

Usage:
    from test.mock_serial import PTYMock
    mock = PTYMock(port_alias='/dev/ttyUSB0')
    mock.start()
    # run the program that opens /dev/ttyUSB0
    mock.stop()

The mock emits frames: [0xFF, DATA_H, DATA_L, SUM] where
SUM = (0xFF + DATA_H + DATA_L) & 0xFF and distance = DATA_H<<8 | DATA_L.
"""

import os
import pty
import threading
import time
import math
import errno
import logging
import grp

DEFAULT_INTERVAL = 0.1
DEFAULT_SERIAL_PORT = '/dev/ttyUSB0'

class PTYMock:
    def __init__(self, port_alias=DEFAULT_SERIAL_PORT, interval=DEFAULT_INTERVAL,
                 min_mm=30, max_mm=2000):
        self.port_alias = port_alias
        self.interval = interval
        self.min_mm = min_mm
        self.max_mm = max_mm

        self.master_fd = None
        self.slave_name = None
        self._stop_event = threading.Event()
        self._thread = None
        self._created_alias = False

    def start(self):
        """Open a PTY and start streaming frames."""
        self.master_fd, slave_fd = pty.openpty()
        # try to assign PTY slave to group "dialout" and make it group-writable
        try:
            try:
                gid = grp.getgrnam('dialout').gr_gid
                # set group on the slave fd (leave owner unchanged)
                os.fchown(slave_fd, -1, gid)
            except (PermissionError, OSError) as e:
                logging.warning(f"Could not chown PTY slave to 'dialout': {e}")
            except KeyError:
                logging.info("Group 'dialout' not present; leaving PTY group as-is")
            try:
                # allow owner+group read/write
                os.fchmod(slave_fd, 0o660)
            except (PermissionError, OSError, AttributeError) as e:
                logging.warning(f"Could not chmod PTY slave: {e}")
        except Exception as e:
            logging.warning(f"Error while setting PTY group/perm: {e}")
        try:
            self.slave_name = os.ttyname(slave_fd)
        except OSError:
            # Fallback if ttyname fails
            self.slave_name = f"/dev/pts/{slave_fd}"

        # Try to create a symlink so code expecting /dev/ttyUSB0 works
        try:
            if os.path.exists(self.port_alias):
                # don't overwrite an existing device; leave it alone
                logging.info(f"Port alias {self.port_alias} already exists, not creating symlink")
            else:
                os.symlink(self.slave_name, self.port_alias)
                self._created_alias = True
                logging.info(f"Created symlink {self.port_alias} -> {self.slave_name}")
        except OSError as e:
            logging.warning(f"Could not create symlink {self.port_alias} -> {self.slave_name}: {e}")

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop streaming and clean up symlink (if created)."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        try:
            if self.master_fd is not None:
                os.close(self.master_fd)
        except OSError:
            pass
        if self._created_alias:
            try:
                os.unlink(self.port_alias)
                logging.info(f"Removed symlink {self.port_alias}")
            except OSError as e:
                logging.warning(f"Failed to remove symlink {self.port_alias}: {e}")

    def _write(self, data: bytes):
        # Write to the master FD; ignore EIO which may happen if slave closed
        try:
            os.write(self.master_fd, data)
        except OSError as e:
            if e.errno in (errno.EIO, errno.EBADF):
                # Slave closed; stop generating
                self._stop_event.set()
            else:
                raise

    def _frame_for_distance(self, distance_mm: int) -> bytes:
        header = 0xFF
        distance = max(0, int(distance_mm)) & 0xFFFF
        data_h = (distance >> 8) & 0xFF
        data_l = distance & 0xFF
        checksum = (header + data_h + data_l) & 0xFF
        return bytes([header, data_h, data_l, checksum])

    def _writer_loop(self):
        """Continuously write frames; distance follows a smooth sine wave."""
        start = time.time()
        while not self._stop_event.is_set():
            t = time.time() - start
            # sine wave between min_mm and max_mm with a period of 10s
            mid = (self.min_mm + self.max_mm) / 2.0
            amp = (self.max_mm - self.min_mm) / 2.0
            distance = mid + amp * math.sin(2 * math.pi * t / 10.0)
            # create a valid frame and write it
            frame = self._frame_for_distance(int(distance))
            self._write(frame)
            time.sleep(self.interval)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    mock = PTYMock(port_alias='/dev/ttyUSB0', interval=0.12, min_mm=30, max_mm=3000)
    try:
        mock.start()
        print(f"Mock PTY slave available at: {mock.slave_name}")
        print(f"(Attempted alias: {mock.port_alias})")
        print("Press Ctrl-C to stop")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping mock")
    finally:
        mock.stop()

