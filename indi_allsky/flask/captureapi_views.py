import subprocess
import threading
import time
import os
import json
import io

from flask import Blueprint, jsonify, request, Response, current_app as app
from flask.views import View
from flask_login import login_required

bp_captureapi_allsky = Blueprint(
    "captureapi_indi_allsky",
    __name__,
    url_prefix="/indi-allsky",
)

CAPTURE_CMD = "rpicam-still"
STREAM_CMD = "rpicam-vid"
CAMERA_ID = 0
HTDOCS = "/var/www/html/allsky/images"
DB_PATH = "/var/lib/indi-allsky/indi-allsky.sqlite"


# ---------------------------------------------------------------------------
# Allsky service helpers
# ---------------------------------------------------------------------------

LOCK_FILE = "/tmp/allsky-stream.lock"


def _stop_allsky():
    """Stop indi-allsky service AND timer to prevent any restart."""
    subprocess.run(["systemctl", "--user", "stop", "indi-allsky.timer"],
                   capture_output=True, timeout=10)
    subprocess.run(["systemctl", "--user", "stop", "indi-allsky"],
                   capture_output=True, timeout=10)
    # Wait for camera to be fully released
    for _ in range(10):
        time.sleep(0.5)
        result = subprocess.run(
            ["rpicam-still", "--list-cameras"],
            capture_output=True, text=True, timeout=5)
        if "imx415" in result.stdout.lower():
            break
    # Write lock file so external tools know camera is claimed
    try:
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
    except OSError:
        pass


def _start_allsky():
    """Restart indi-allsky service and timer (only if stream is NOT running)."""
    if _stream.running:
        return  # stream owns the camera, don't restart allsky
    # Remove lock file
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass
    subprocess.run(["systemctl", "--user", "start", "indi-allsky.timer"],
                   capture_output=True, timeout=10)
    subprocess.run(["systemctl", "--user", "start", "indi-allsky"],
                   capture_output=True, timeout=10)


# ---------------------------------------------------------------------------
# Single-shot capture (Capture One button)
# ---------------------------------------------------------------------------

def _capture(output_path, quality=90, width=None, height=None,
             shutter=None, gain=None, awb=None, brightness=None,
             contrast=None, saturation=None, sharpness=None,
             denoise=None, ev=None, awbgains=None):
    cmd = [CAPTURE_CMD, "--immediate", "--nopreview",
           "--camera", str(CAMERA_ID), "--encoding", "jpg",
           "--quality", str(quality), "--output", output_path]
    if width and height:
        cmd.extend(["--width", str(width), "--height", str(height)])
    if shutter is not None and shutter > 0:
        cmd.extend(["--shutter", str(int(shutter))])
    if gain is not None and gain > 0:
        cmd.extend(["--gain", str(gain)])
    if awbgains:
        cmd.extend(["--awbgains", awbgains])
    elif awb:
        cmd.extend(["--awb", awb])
    else:
        cmd.extend(["--awb", "auto"])
    if brightness is not None:
        cmd.extend(["--brightness", str(brightness)])
    if contrast is not None:
        cmd.extend(["--contrast", str(contrast)])
    if saturation is not None:
        cmd.extend(["--saturation", str(saturation)])
    if sharpness is not None:
        cmd.extend(["--sharpness", str(sharpness)])
    if denoise:
        cmd.extend(["--denoise", denoise])
    if ev is not None:
        cmd.extend(["--ev", str(ev)])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False, result.stderr
    return True, ""


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _get_config():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT data FROM config ORDER BY createDate DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if row:
        return json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return {}


def _save_config(config_data, note="slider update"):
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    cur.execute(
        "INSERT INTO config (createDate, level, encrypted, note, data) VALUES (?, 'user', 0, ?, ?)",
        (now, note, json.dumps(config_data)),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# MJPEG Stream Manager - single rpicam-vid process, multiple clients
# ---------------------------------------------------------------------------

class MJPEGStreamManager:
    """Manages a single rpicam-vid process and fans JPEG frames to all clients."""

    def __init__(self):
        self._lock = threading.Lock()
        self._proc = None
        self._reader_thread = None
        self._frame = None           # latest JPEG bytes
        self._frame_event = threading.Event()
        self._client_count = 0
        self._settings = {}
        self._running = False

    @property
    def running(self):
        return self._running and self._proc is not None and self._proc.poll() is None

    @property
    def client_count(self):
        return self._client_count

    def _build_cmd(self, settings):
        cmd = [STREAM_CMD, "--nopreview", "--camera", str(CAMERA_ID),
               "--codec", "mjpeg", "--quality", "75",
               "--width", "1920", "--height", "1080",
               "--framerate", str(settings.get("framerate", 10)),
               "--timeout", "0",        # run indefinitely
               "--output", "-"]         # stdout
        s = settings
        if s.get("shutter") and int(s["shutter"]) > 0:
            cmd.extend(["--shutter", str(int(s["shutter"]))])
        if s.get("gain") and float(s["gain"]) > 0:
            cmd.extend(["--gain", str(s["gain"])])
        if s.get("awbgains"):
            cmd.extend(["--awbgains", str(s["awbgains"])])
        elif s.get("awb"):
            cmd.extend(["--awb", str(s["awb"])])
        else:
            cmd.extend(["--awb", "auto"])
        if s.get("brightness") is not None:
            cmd.extend(["--brightness", str(s["brightness"])])
        if s.get("contrast") is not None:
            cmd.extend(["--contrast", str(s["contrast"])])
        if s.get("saturation") is not None:
            cmd.extend(["--saturation", str(s["saturation"])])
        if s.get("sharpness") is not None:
            cmd.extend(["--sharpness", str(s["sharpness"])])
        if s.get("denoise"):
            cmd.extend(["--denoise", str(s["denoise"])])
        return cmd

    def start(self, settings=None):
        """Start rpicam-vid. Returns (ok, error_msg) tuple."""
        with self._lock:
            if settings is None:
                settings = {}
            self._settings = settings
            self._stop_proc()
            _stop_allsky()
            self._frame = None
            self._last_error = None
            cmd = self._build_cmd(settings)
            try:
                self._proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    bufsize=0,
                )
            except Exception as e:
                self._running = False
                return False, "Failed to launch rpicam-vid: " + str(e)
            self._running = True
            self._reader_thread = threading.Thread(
                target=self._read_frames, daemon=True)
            self._reader_thread.start()
        # Wait for the first frame before returning (up to 8s)
        for _ in range(80):
            if self._frame is not None:
                return True, ""
            # Check if process died early
            if self._proc and self._proc.poll() is not None:
                stderr = ""
                try:
                    stderr = self._proc.stderr.read().decode("utf-8", errors="replace")[:500]
                except Exception:
                    pass
                self._running = False
                return False, "rpicam-vid exited (code {}): {}".format(
                    self._proc.returncode, stderr.strip() or "no output")
            time.sleep(0.1)
        # Timed out waiting for first frame
        if self._proc and self._proc.poll() is None:
            # Process is running but no frames yet
            stderr_peek = ""
            try:
                stderr_peek = self._proc.stderr.read1(2048).decode("utf-8", errors="replace") if hasattr(self._proc.stderr, 'read1') else ""
            except Exception:
                pass
            self._stop_proc()
            self._running = False
            return False, "rpicam-vid started but produced no frames within 8s. stderr: " + (stderr_peek.strip() or "empty")
        self._running = False
        return False, "rpicam-vid failed to start (process gone)"

    def stop(self):
        with self._lock:
            self._stop_proc()
            self._running = False
            _start_allsky()

    def update_settings(self, settings):
        """Restart rpicam-vid with new settings. Returns (ok, error_msg)."""
        with self._lock:
            self._settings.update(settings)
            if self._running:
                self._stop_proc()
                self._frame = None
                cmd = self._build_cmd(self._settings)
                try:
                    self._proc = subprocess.Popen(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        bufsize=0,
                    )
                except Exception as e:
                    self._running = False
                    return False, "Failed to restart rpicam-vid: " + str(e)
                self._reader_thread = threading.Thread(
                    target=self._read_frames, daemon=True)
                self._reader_thread.start()
        # Wait for first frame (up to 8s)
        for _ in range(80):
            if self._frame is not None:
                return True, ""
            if self._proc and self._proc.poll() is not None:
                stderr = ""
                try:
                    stderr = self._proc.stderr.read().decode("utf-8", errors="replace")[:500]
                except Exception:
                    pass
                self._running = False
                return False, "rpicam-vid exited (code {}): {}".format(
                    self._proc.returncode, stderr.strip() or "no output")
            time.sleep(0.1)
        return True, ""  # still running, just slow to produce frames

    def _stop_proc(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=2)
        self._proc = None

    def _read_frames(self):
        """Read MJPEG stream from stdout, extract JPEG frames."""
        proc = self._proc
        buf = bytearray()
        SOI = b'\xff\xd8'
        EOI = b'\xff\xd9'
        try:
            while proc and proc.poll() is None and self._running:
                chunk = proc.stdout.read(65536)
                if not chunk:
                    break
                buf.extend(chunk)
                # Extract complete JPEG frames
                while True:
                    start = buf.find(SOI)
                    if start < 0:
                        buf.clear()
                        break
                    end = buf.find(EOI, start + 2)
                    if end < 0:
                        # Trim anything before SOI to save memory
                        if start > 0:
                            del buf[:start]
                        break
                    frame = bytes(buf[start:end + 2])
                    del buf[:end + 2]
                    self._frame = frame
                    self._frame_event.set()
                    self._frame_event.clear()
        except Exception:
            pass

    def get_frame(self, timeout=5.0):
        """Wait for the next frame (or return latest immediately)."""
        if self._frame is not None:
            return self._frame
        self._frame_event.wait(timeout=timeout)
        return self._frame

    def generate(self):
        """Generator yielding multipart MJPEG frames for a streaming response."""
        self._client_count += 1
        try:
            last_frame = None
            while self._running and self._proc and self._proc.poll() is None:
                frame = self._frame
                if frame is not None and frame is not last_frame:
                    last_frame = frame
                    yield (b'--frame\r\n'
                           b'Content-Type: image/jpeg\r\n'
                           b'Content-Length: ' + str(len(frame)).encode() + b'\r\n'
                           b'\r\n' + frame + b'\r\n')
                else:
                    time.sleep(0.03)  # ~30Hz poll
        finally:
            self._client_count -= 1

    def get_settings(self):
        return dict(self._settings)


# Global stream manager instance
_stream = MJPEGStreamManager()


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class ConfigGetMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        try:
            cfg = _get_config()
            ccd = cfg.get("CCD_CONFIG", {})
            result = {
                "NIGHT_GAIN": ccd.get("NIGHT", {}).get("GAIN", 0),
                "MOONMODE_GAIN": ccd.get("MOONMODE", {}).get("GAIN", 0),
                "DAY_GAIN": ccd.get("DAY", {}).get("GAIN", 0),
                "CCD_EXPOSURE_MAX": cfg.get("CCD_EXPOSURE_MAX", 30),
                "CCD_EXPOSURE_DEF": cfg.get("CCD_EXPOSURE_DEF", 5),
                "TARGET_ADU": cfg.get("TARGET_ADU", 75),
                "TARGET_ADU_DAY": cfg.get("TARGET_ADU_DAY", 100),
                "SATURATION_FACTOR": cfg.get("SATURATION_FACTOR", 1.0),
                "SATURATION_FACTOR_DAY": cfg.get("SATURATION_FACTOR_DAY", 1.0),
                "GAMMA_CORRECTION": cfg.get("GAMMA_CORRECTION", 1.0),
                "GAMMA_CORRECTION_DAY": cfg.get("GAMMA_CORRECTION_DAY", 1.0),
                "SHARPEN_AMOUNT": cfg.get("SHARPEN_AMOUNT", 0.0),
                "SHARPEN_AMOUNT_DAY": cfg.get("SHARPEN_AMOUNT_DAY", 0.0),
                "IMAGE_FLIP_V": cfg.get("IMAGE_FLIP_V", False),
                "IMAGE_FLIP_H": cfg.get("IMAGE_FLIP_H", False),
                "EXPOSURE_PERIOD": cfg.get("EXPOSURE_PERIOD", 35),
                "EXPOSURE_PERIOD_DAY": cfg.get("EXPOSURE_PERIOD_DAY", 15),
            }
            return jsonify(result), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500


class ConfigSetMethodView(View):
    decorators = [login_required]
    methods = ["POST"]

    def dispatch_request(self):
        try:
            updates = request.get_json(force=True)
            if not updates:
                return jsonify({"error": "No data"}), 400
            cfg = _get_config()
            ccd = cfg.setdefault("CCD_CONFIG", {})
            key_map = {
                "NIGHT_GAIN": lambda v: ccd.setdefault("NIGHT", {}).update({"GAIN": float(v)}),
                "MOONMODE_GAIN": lambda v: ccd.setdefault("MOONMODE", {}).update({"GAIN": float(v)}),
                "DAY_GAIN": lambda v: ccd.setdefault("DAY", {}).update({"GAIN": float(v)}),
            }
            flat_keys = [
                "CCD_EXPOSURE_MAX", "CCD_EXPOSURE_DEF", "EXPOSURE_PERIOD", "EXPOSURE_PERIOD_DAY",
                "SATURATION_FACTOR", "SATURATION_FACTOR_DAY",
                "GAMMA_CORRECTION", "GAMMA_CORRECTION_DAY",
                "SHARPEN_AMOUNT", "SHARPEN_AMOUNT_DAY",
            ]
            int_keys = ["TARGET_ADU", "TARGET_ADU_DAY"]
            bool_keys = ["IMAGE_FLIP_V", "IMAGE_FLIP_H"]
            changed = []
            for k, v in updates.items():
                if k in key_map:
                    key_map[k](v)
                    changed.append(k)
                elif k in flat_keys:
                    cfg[k] = float(v)
                    changed.append(k)
                elif k in int_keys:
                    cfg[k] = int(v)
                    changed.append(k)
                elif k in bool_keys:
                    cfg[k] = bool(v)
                    changed.append(k)
            if changed:
                _save_config(cfg, note="live slider: " + ", ".join(changed))
            return jsonify({"message": "Config updated", "changed": changed}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500


class CaptureOneMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        try:
            dest = os.path.join(HTDOCS, "capture_one.jpg")

            # If stream is running, grab a frame from it (no rpicam-still needed)
            if _stream.running:
                frame = _stream.get_frame(timeout=5)
                if frame is None:
                    return jsonify({"error": "Stream active but no frame available"}), 500
                with open(dest, "wb") as f:
                    f.write(frame)
                return jsonify({
                    "url": "/indi-allsky/images/capture_one.jpg?t=" + str(int(time.time())),
                    "message": "Captured from stream",
                }), 200

            # No stream running - stop allsky, do a still capture, restart
            _stop_allsky()
            ok, err = _capture(dest, quality=90,
                               shutter=request.args.get("shutter", None, type=int),
                               gain=request.args.get("gain", None, type=float),
                               awb=request.args.get("awb", None),
                               brightness=request.args.get("brightness", None, type=float),
                               contrast=request.args.get("contrast", None, type=float),
                               saturation=request.args.get("saturation", None, type=float),
                               sharpness=request.args.get("sharpness", None, type=float),
                               awbgains=request.args.get("awbgains", None))
            _start_allsky()

            if not ok:
                return jsonify({"error": err}), 500
            return jsonify({
                "url": "/indi-allsky/images/capture_one.jpg?t=" + str(int(time.time())),
                "message": "Captured",
            }), 200
        except subprocess.TimeoutExpired:
            _start_allsky()
            return jsonify({"error": "Capture timed out"}), 500
        except Exception as e:
            _start_allsky()
            return jsonify({"error": str(e)}), 500


class StreamStartMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        settings = {}
        for key in ["shutter", "gain", "brightness", "contrast",
                     "saturation", "sharpness", "awb", "awbgains",
                     "denoise", "framerate"]:
            val = request.args.get(key, None)
            if val is not None:
                settings[key] = val
        ok, err = _stream.start(settings)
        if not ok:
            return jsonify({"error": err}), 500
        return jsonify({"message": "Stream started",
                        "clients": _stream.client_count}), 200


class StreamStopMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        _stream.stop()
        return jsonify({"message": "Stream stopped, allsky restarting"}), 200


class StreamUpdateMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        """Update stream settings on the fly - restarts rpicam-vid."""
        settings = {}
        for key in ["shutter", "gain", "brightness", "contrast",
                     "saturation", "sharpness", "awb", "awbgains",
                     "denoise", "framerate"]:
            val = request.args.get(key, None)
            if val is not None:
                settings[key] = val
        if not _stream.running:
            return jsonify({"error": "Stream not running"}), 400
        ok, err = _stream.update_settings(settings)
        if not ok:
            return jsonify({"error": err}), 500
        return jsonify({"message": "Settings updated",
                        "clients": _stream.client_count}), 200


class StreamMJPEGMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        """MJPEG multipart stream - connect <img src> directly to this."""
        # Wait up to 5s for stream to be ready
        for _ in range(50):
            if _stream.running and _stream._frame is not None:
                break
            time.sleep(0.1)
        if not _stream.running:
            return jsonify({"error": "Stream not running. Start it first."}), 400
        return Response(
            _stream.generate(),
            mimetype='multipart/x-mixed-replace; boundary=frame',
        )


class StreamStatusMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        return jsonify({
            "running": _stream.running,
            "clients": _stream.client_count,
            "settings": _stream.get_settings(),
        }), 200


# Keep old endpoints for backwards compat (they still work)
class LiveStartMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        settings = {}
        for key in ["shutter", "gain", "brightness", "contrast",
                     "saturation", "sharpness", "awb", "awbgains", "denoise"]:
            val = request.args.get(key, None)
            if val is not None:
                settings[key] = val
        ok, err = _stream.start(settings)
        if not ok:
            return jsonify({"error": err}), 500
        return jsonify({"message": "Live mode started (MJPEG stream)"}), 200


class LiveStopMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        _stream.stop()
        return jsonify({"message": "Live mode stopped"}), 200


class LiveFrameMethodView(View):
    decorators = [login_required]
    methods = ["GET"]

    def dispatch_request(self):
        """Legacy: return latest frame as a single JPEG URL."""
        if not _stream.running:
            return jsonify({"error": "Stream not active"}), 400
        frame = _stream.get_frame(timeout=5)
        if frame is None:
            return jsonify({"error": "No frame available"}), 500
        # Write frame to htdocs for URL access
        dest = os.path.join(HTDOCS, "live_frame.jpg")
        with open(dest, "wb") as f:
            f.write(frame)
        return jsonify({
            "url": "/indi-allsky/images/live_frame.jpg?t=" + str(int(time.time())),
            "message": "Live",
        }), 200


# ---------------------------------------------------------------------------
# URL rules
# ---------------------------------------------------------------------------

bp_captureapi_allsky.add_url_rule("/api/capture_one", view_func=CaptureOneMethodView.as_view("captureapi_capture_one"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/live_start", view_func=LiveStartMethodView.as_view("captureapi_live_start"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/live_stop", view_func=LiveStopMethodView.as_view("captureapi_live_stop"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/live_frame", view_func=LiveFrameMethodView.as_view("captureapi_live_frame"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/stream/start", view_func=StreamStartMethodView.as_view("captureapi_stream_start"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/stream/stop", view_func=StreamStopMethodView.as_view("captureapi_stream_stop"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/stream/update", view_func=StreamUpdateMethodView.as_view("captureapi_stream_update"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/stream/feed.mjpeg", view_func=StreamMJPEGMethodView.as_view("captureapi_stream_feed"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/stream/status", view_func=StreamStatusMethodView.as_view("captureapi_stream_status"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/config_get", view_func=ConfigGetMethodView.as_view("captureapi_config_get"), methods=["GET"])
bp_captureapi_allsky.add_url_rule("/api/config_set", view_func=ConfigSetMethodView.as_view("captureapi_config_set"), methods=["POST"])
