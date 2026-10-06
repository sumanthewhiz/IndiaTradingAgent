from __future__ import annotations

import ctypes
import json
import os
import tempfile
import threading
import time as clock
from ctypes import wintypes
from pathlib import Path
from typing import Protocol

from .core import SafetyError, now_ist

KEY_FIELDS = ("ai_api_key", "broker_api_key", "broker_api_secret", "broker_access_token")


class Protector(Protocol):
    def protect(self, plain: bytes) -> bytes: ...
    def unprotect(self, encrypted: bytes) -> bytes: ...


class WindowsProtector:
    """DPAPI CurrentUser encryption. There is deliberately no plaintext fallback."""

    def _convert(self, data: bytes, decrypt: bool) -> bytes:
        if os.name != "nt":
            raise SafetyError("Persistent credentials require Windows DPAPI on this release.")

        class Blob(ctypes.Structure):
            _fields_ = [("length", wintypes.DWORD), ("data", ctypes.POINTER(ctypes.c_ubyte))]

        buffer = ctypes.create_string_buffer(data)
        incoming = Blob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
        outgoing = Blob()
        crypt = ctypes.WinDLL("crypt32", use_last_error=True)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        operation = crypt.CryptUnprotectData if decrypt else crypt.CryptProtectData
        operation.argtypes = [
            ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
            ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(Blob),
        ]
        operation.restype = wintypes.BOOL
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree.restype = ctypes.c_void_p
        if not operation(ctypes.byref(incoming), None, None, None, None, 1, ctypes.byref(outgoing)):
            raise SafetyError("Windows credential protection failed; use the original Windows user/profile.")
        try:
            return ctypes.string_at(outgoing.data, outgoing.length)
        finally:
            kernel.LocalFree(outgoing.data)

    def protect(self, plain: bytes) -> bytes:
        return self._convert(plain, False)

    def unprotect(self, encrypted: bytes) -> bytes:
        return self._convert(encrypted, True)


def read_atomic_json(path: Path) -> dict:
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SafetyError(f"Invalid local state object: {path.name}.")
    return value


def atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + ".", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        for delay in (0.01, 0.02, 0.04, 0.08, 0.16, None):
            try:
                os.replace(temporary, path)
                break
            except PermissionError as exc:
                if os.name != "nt" or getattr(exc, "winerror", None) not in {5, 32, 33} or delay is None:
                    raise
                clock.sleep(delay)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def atomic_json(path: Path, value: dict) -> None:
    atomic_bytes(path, json.dumps(value, ensure_ascii=True, allow_nan=False).encode("utf-8"))


def application_directory() -> Path:
    if os.name != "nt" or not os.environ.get("LOCALAPPDATA"):
        raise SafetyError("The dashboard requires Windows with a LOCALAPPDATA directory.")
    return Path(os.environ["LOCALAPPDATA"]) / "IndiaTradingAgent"


class CredentialVault:
    def __init__(self, path: Path, protector: Protector | None = None):
        self.path = path
        self.protector = protector or WindowsProtector()
        self.lock = threading.RLock()

    def load(self) -> dict:
        with self.lock:
            if not self.path.exists():
                return {"version": 1, "keys": {}, "auto_start": False, "saved_at": None}
            encrypted = self.path.read_bytes()
            if not encrypted.startswith(b"ITA-VAULT-1\n") or len(encrypted) > 65536:
                raise SafetyError("Credential vault is invalid. It was not reset or read as plaintext.")
            try:
                value = json.loads(self.protector.unprotect(encrypted[12:]))
            except (ValueError, UnicodeError):
                raise SafetyError("Credential vault contents could not be decoded.") from None
            if (value.get("version") != 1 or not isinstance(value.get("keys"), dict)
                    or type(value.get("auto_start")) is not bool):
                raise SafetyError("Credential vault has an unsupported format.")
            return value

    def _write(self, value: dict) -> None:
        encoded = json.dumps(value, sort_keys=True, allow_nan=False).encode()
        atomic_bytes(self.path, b"ITA-VAULT-1\n" + self.protector.protect(encoded))

    def save(self, changes: dict, authorize_live: bool) -> dict:
        if set(changes) - set(KEY_FIELDS):
            raise SafetyError("Only AI and broker API credential fields are accepted.")
        if authorize_live is not True:
            raise SafetyError("Saving credentials for automatic live trading requires explicit authorization.")
        with self.lock:
            value = self.load()
            for name, raw in changes.items():
                if type(raw) is not str or len(raw) > 8192:
                    raise SafetyError("Invalid credential field.")
                key = raw.strip()
                if any(character.isspace() or ord(character) < 32 for character in key):
                    raise SafetyError("API keys/tokens must not contain whitespace or control characters.")
                if key:
                    value["keys"][name] = key
            if not value["keys"].get("ai_api_key") or not value["keys"].get("broker_api_key"):
                raise SafetyError("A Gemini API key and a Kite API key are required.")
            if not (value["keys"].get("broker_api_secret") or value["keys"].get("broker_access_token")):
                raise SafetyError("Provide the Kite API secret for broker sign-in, or a current access token.")
            value.update(auto_start=True, saved_at=now_ist().isoformat(), consent_version="auto-live-v1")
            self._write(value)
            return self.public_status()

    def set_auto_start(self, enabled: bool) -> None:
        with self.lock:
            value = self.load()
            value["auto_start"] = bool(enabled)
            self._write(value)

    def set_access_token(self, token: str) -> None:
        if not token or len(token) > 8192 or any(x.isspace() for x in token):
            raise SafetyError("Broker returned an invalid access token.")
        with self.lock:
            value = self.load()
            value["keys"]["broker_access_token"] = token
            value["token_saved_at"] = now_ist().isoformat()
            self._write(value)

    def public_status(self) -> dict:
        value = self.load()
        return {
            "saved": {key: bool(value["keys"].get(key)) for key in KEY_FIELDS},
            "auto_start": value["auto_start"], "saved_at": value["saved_at"],
            "protection": "Windows DPAPI / current Windows user",
        }

    def forget(self) -> None:
        with self.lock:
            if self.path.exists():
                self.path.unlink()
