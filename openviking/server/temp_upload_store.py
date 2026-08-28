# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Temporary upload storage backends for HTTP server uploads."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, Optional

from openviking.server.config import ServerConfig, TempUploadConfig
from openviking.server.identity import RequestContext, Role
from openviking.server.local_input_guard import _read_upload_meta
from openviking.storage.viking_fs import LS_ALL_NODES, get_viking_fs
from openviking_cli.exceptions import InvalidArgumentError, PermissionDeniedError
from openviking_cli.utils.config.open_viking_config import get_openviking_config

_CHUNK_SIZE = 1024 * 1024
_SHARED_UPLOAD_ROOT = "viking://upload"
_SHARED_CLEANUP_COOLDOWN_TTL_DIVISOR = 2
_SHARED_CLEANUP_QUEUE_MAX_SIZE = 100
_SHARED_CLEANUP_SLEEP_EVERY_REMOVALS = 10
_SHARED_CLEANUP_SLEEP_SECONDS = 0.2
_SHARED_CLEANUP_QUEUE: Queue[tuple["TempUploadStore", RequestContext, str]] = Queue(
    maxsize=_SHARED_CLEANUP_QUEUE_MAX_SIZE
)
_SHARED_CLEANUP_WORKER_STARTED = False
_SHARED_CLEANUP_WORKER_LOCK = Lock()
_SHARED_CLEANUP_STATES: dict[str, "_SharedCleanupState"] = {}
_SHARED_CLEANUP_STATES_LOCK = Lock()

logger = logging.getLogger(__name__)


@dataclass
class _SharedCleanupState:
    inflight: bool = False
    last_success_at: Optional[float] = None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f)


def _write_bytes(path: str, content: bytes) -> None:
    with open(path, "wb") as f:
        f.write(content)


def _open_binary_for_write(path: str | Path):
    return open(path, "wb")


def _close_file(file_obj: Any) -> None:
    file_obj.close()


def _create_temp_file(*, prefix: str, suffix: str) -> str:
    fd, temp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    return temp_path


@dataclass
class ResolvedTempUpload:
    mode: str
    temp_file_id: str
    original_filename: Optional[str]
    local_path: str

    async def cleanup(self) -> None:
        if self.mode == "shared" and self.local_path:
            with suppress(FileNotFoundError):
                await asyncio.to_thread(os.unlink, self.local_path)


def get_temp_upload_config(server_config: ServerConfig) -> TempUploadConfig:
    return server_config.temp_upload


def _shared_content_uri(upload_id: str) -> str:
    return f"{_SHARED_UPLOAD_ROOT}/{upload_id}/content"


def _shared_meta_uri(upload_id: str) -> str:
    return f"{_SHARED_UPLOAD_ROOT}/{upload_id}/meta"


def _parse_shared_temp_file_id(temp_file_id: str) -> Optional[str]:
    if not temp_file_id.startswith("shared_"):
        return None
    upload_id = temp_file_id[len("shared_") :].strip()
    if not upload_id or "/" in upload_id or "\\" in upload_id:
        return None
    return upload_id


def _new_shared_upload_id() -> str:
    return f"{time.time_ns() // 1_000_000:013d}-{uuid.uuid4().hex}"


def _shared_upload_created_at(upload_id: str) -> Optional[float]:
    timestamp_ms, separator, nonce = upload_id.partition("-")
    if (
        not separator
        or len(timestamp_ms) != 13
        or not timestamp_ms.isdigit()
        or len(nonce) != 32
        or any(char not in "0123456789abcdef" for char in nonce)
    ):
        return None
    return int(timestamp_ms) / 1_000


async def _stream_upload_to_local_temp(upload_file: Any, max_size_bytes: int) -> tuple[str, int]:
    suffix = Path(upload_file.filename or "upload.tmp").suffix or ".tmp"
    temp_path = await asyncio.to_thread(
        _create_temp_file, prefix="ov_http_upload_", suffix=suffix
    )
    total = 0
    f = None
    try:
        f = await asyncio.to_thread(_open_binary_for_write, temp_path)
        while True:
            chunk = await upload_file.read(_CHUNK_SIZE)
            if not chunk:
                break
            total += len(chunk)
            if total > max_size_bytes:
                raise InvalidArgumentError(
                    f"Upload exceeds size limit ({max_size_bytes} bytes)."
                )
            # UploadFile reads already yield to the event loop.  Local disk writes do
            # not, so run each bounded write in the default executor rather than
            # stalling the Core worker event loop on slow local storage.
            await asyncio.to_thread(f.write, chunk)
        return temp_path, total
    except Exception:
        with suppress(FileNotFoundError):
            await asyncio.to_thread(os.unlink, temp_path)
        raise
    finally:
        if f is not None:
            await asyncio.to_thread(_close_file, f)


class TempUploadStore:
    def __init__(self, server_config: ServerConfig):
        self.server_config = server_config
        self.temp_cfg = get_temp_upload_config(server_config)

    @staticmethod
    def build(server_config: ServerConfig) -> "TempUploadStore":
        return TempUploadStore(server_config)

    @staticmethod
    def _internal_ctx(ctx: RequestContext) -> RequestContext:
        return RequestContext(
            user=ctx.user,
            role=Role.ROOT,
        )

    async def save_upload(
        self,
        upload_file: Any,
        upload_mode: str,
        ctx: RequestContext,
    ) -> str:
        if upload_mode == "local":
            return await self._save_local(upload_file)
        if upload_mode == "shared":
            return await self._save_shared(upload_file, ctx)
        raise InvalidArgumentError("upload_mode must be 'local' or 'shared'.")

    async def resolve_for_consume(
        self,
        temp_file_id: str,
        ctx: RequestContext,
    ) -> ResolvedTempUpload:
        shared_id = _parse_shared_temp_file_id(temp_file_id)
        if shared_id is None:
            return await asyncio.to_thread(self._resolve_local, temp_file_id)
        return await self._resolve_shared(temp_file_id, shared_id, ctx)

    async def _save_local(self, upload_file: Any) -> str:
        config = get_openviking_config()
        temp_dir = config.storage.get_upload_temp_dir()
        await asyncio.to_thread(self._cleanup_local_temp_files, temp_dir)

        file_ext = Path(upload_file.filename).suffix if upload_file.filename else ".tmp"
        temp_filename = f"upload_{uuid.uuid4().hex}{file_ext}"
        temp_file_path = temp_dir / temp_filename

        total = 0
        f = await asyncio.to_thread(_open_binary_for_write, temp_file_path)
        try:
            while True:
                chunk = await upload_file.read(_CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > self.temp_cfg.shared_max_size_bytes:
                    with suppress(FileNotFoundError):
                        await asyncio.to_thread(temp_file_path.unlink)
                    raise InvalidArgumentError(
                        f"Upload exceeds size limit ({self.temp_cfg.shared_max_size_bytes} bytes)."
                    )
                await asyncio.to_thread(f.write, chunk)
        finally:
            await asyncio.to_thread(_close_file, f)

        if upload_file.filename:
            meta_path = temp_dir / f"{temp_filename}.ov_upload.meta"
            meta = {
                "original_filename": upload_file.filename,
                "upload_time": time.time(),
            }
            await asyncio.to_thread(_write_json, meta_path, meta)

        return temp_filename

    async def _save_shared(self, upload_file: Any, ctx: RequestContext) -> str:
        temp_path, total_size = await _stream_upload_to_local_temp(
            upload_file, self.temp_cfg.shared_max_size_bytes
        )
        upload_id = _new_shared_upload_id()
        temp_file_id = f"shared_{upload_id}"
        vfs = get_viking_fs()
        internal_ctx = self._internal_ctx(ctx)
        content_uri = _shared_content_uri(upload_id)
        meta_uri = _shared_meta_uri(upload_id)
        meta = {
            "version": 2,
            "temp_file_id": temp_file_id,
            "account": ctx.account_id,
            "user": ctx.user.user_id,
            "original_filename": upload_file.filename or "",
            "content_type": getattr(upload_file, "content_type", None),
            "file_ext": Path(upload_file.filename or "").suffix,
            "size": total_size,
            "storage_uri": content_uri,
        }

        try:
            content = await asyncio.to_thread(Path(temp_path).read_bytes)
            await vfs.write_file_bytes(content_uri, content, ctx=internal_ctx)
            await vfs.write_file(meta_uri, json.dumps(meta, ensure_ascii=False), ctx=internal_ctx)
            self._schedule_shared_cleanup(ctx)
            return temp_file_id
        except Exception:
            with suppress(Exception):
                await vfs.rm(
                    f"{_SHARED_UPLOAD_ROOT}/{upload_id}",
                    recursive=True,
                    ctx=internal_ctx,
                )
            raise
        finally:
            with suppress(FileNotFoundError):
                await asyncio.to_thread(os.unlink, temp_path)

    def _schedule_shared_cleanup(self, ctx: RequestContext) -> None:
        """Run the best-effort shared-upload cleanup off the request path."""
        ttl_seconds = self.temp_cfg.ttl_seconds
        if ttl_seconds == 0:
            return

        account_id = ctx.account_id
        now = time.monotonic()
        cooldown_seconds = ttl_seconds / _SHARED_CLEANUP_COOLDOWN_TTL_DIVISOR
        with _SHARED_CLEANUP_STATES_LOCK:
            state = _SHARED_CLEANUP_STATES.setdefault(account_id, _SharedCleanupState())
            if state.inflight:
                logger.debug(
                    "[TempUpload] Shared cleanup already inflight account=%s", account_id
                )
                return
            if (
                state.last_success_at is not None
                and now - state.last_success_at < cooldown_seconds
            ):
                logger.debug(
                    "[TempUpload] Shared cleanup cooling down account=%s cooldown_seconds=%s",
                    account_id,
                    cooldown_seconds,
                )
                return
            state.inflight = True
            try:
                _SHARED_CLEANUP_QUEUE.put_nowait((self, ctx, account_id))
            except Full:
                state.inflight = False
                logger.warning(
                    "[TempUpload] Shared cleanup queue full account=%s queue_max_size=%s",
                    account_id,
                    _SHARED_CLEANUP_QUEUE_MAX_SIZE,
                )
                return

        self._ensure_shared_cleanup_worker()

    @staticmethod
    def _ensure_shared_cleanup_worker() -> None:
        global _SHARED_CLEANUP_WORKER_STARTED
        with _SHARED_CLEANUP_WORKER_LOCK:
            if _SHARED_CLEANUP_WORKER_STARTED:
                return
            Thread(
                target=TempUploadStore._run_shared_cleanup_worker,
                name="ov-shared-upload-cleanup",
                daemon=True,
            ).start()
            _SHARED_CLEANUP_WORKER_STARTED = True

    @staticmethod
    def _run_shared_cleanup_worker() -> None:
        while True:
            store, ctx, account_id = _SHARED_CLEANUP_QUEUE.get()
            succeeded = False
            started_at = time.monotonic()
            try:
                scanned_count, removed_count, failed_count = asyncio.run(
                    store._cleanup_shared_uploads(ctx)
                )
                succeeded = True
                logger.info(
                    "[TempUpload] Shared cleanup completed account=%s elapsed_ms=%.1f "
                    "scanned_count=%s removed_count=%s failed_count=%s",
                    account_id,
                    (time.monotonic() - started_at) * 1000.0,
                    scanned_count,
                    removed_count,
                    failed_count,
                )
            except Exception:
                logger.warning(
                    "[TempUpload] Shared cleanup failed account=%s",
                    account_id,
                    exc_info=True,
                )
            finally:
                with _SHARED_CLEANUP_STATES_LOCK:
                    state = _SHARED_CLEANUP_STATES.get(account_id)
                    if state is not None:
                        state.inflight = False
                        if succeeded:
                            state.last_success_at = time.monotonic()
                _SHARED_CLEANUP_QUEUE.task_done()

    def _resolve_local(self, temp_file_id: str) -> ResolvedTempUpload:
        upload_temp_dir = get_openviking_config().storage.get_upload_temp_dir()
        if not temp_file_id or temp_file_id in {".", ".."}:
            raise PermissionDeniedError(
                "HTTP server only accepts regular files from the upload temp directory."
            )
        raw_name = Path(temp_file_id)
        if raw_name.name != temp_file_id or "/" in temp_file_id or "\\" in temp_file_id:
            raise PermissionDeniedError(
                "HTTP server only accepts temp_file_id values issued from the upload temp directory."
            )

        raw_path = upload_temp_dir / temp_file_id
        if raw_path.is_symlink():
            raise PermissionDeniedError(
                "HTTP server only accepts regular files from the upload temp directory."
            )

        try:
            resolved_path = raw_path.resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise PermissionDeniedError(
                "HTTP server only accepts regular files from the upload temp directory."
            ) from exc

        upload_root = upload_temp_dir.resolve()
        try:
            resolved_path.relative_to(upload_root)
        except ValueError as exc:
            raise PermissionDeniedError(
                "HTTP server only accepts temp_file_id values issued from the upload temp directory."
            ) from exc

        if not resolved_path.is_file():
            raise PermissionDeniedError(
                "HTTP server only accepts regular files from the upload temp directory."
            )

        meta_path = upload_temp_dir / f"{temp_file_id}.ov_upload.meta"
        meta = _read_upload_meta(meta_path)
        original_filename = meta.get("original_filename") if meta else None
        return ResolvedTempUpload(
            mode="local",
            temp_file_id=temp_file_id,
            original_filename=original_filename,
            local_path=str(resolved_path),
        )

    async def _resolve_shared(
        self,
        temp_file_id: str,
        upload_id: str,
        ctx: RequestContext,
    ) -> ResolvedTempUpload:
        meta = await self._read_shared_meta(upload_id, ctx)
        self._validate_shared_meta(meta, temp_file_id, ctx)

        content_uri = meta["storage_uri"]
        vfs = get_viking_fs()
        internal_ctx = self._internal_ctx(ctx)
        if not await vfs.exists(content_uri, ctx=internal_ctx):
            raise PermissionDeniedError("Temporary upload is invalid: content missing.")

        file_ext = meta.get("file_ext") or ".tmp"
        temp_path = await asyncio.to_thread(
            _create_temp_file, prefix="ov_shared_upload_", suffix=file_ext
        )
        try:
            content = await vfs.read_file_bytes(content_uri, ctx=internal_ctx)
            await asyncio.to_thread(_write_bytes, temp_path, content)
        except Exception:
            with suppress(FileNotFoundError):
                await asyncio.to_thread(os.unlink, temp_path)
            raise
        return ResolvedTempUpload(
            mode="shared",
            temp_file_id=temp_file_id,
            original_filename=meta.get("original_filename") or None,
            local_path=temp_path,
        )

    async def _read_shared_meta(self, upload_id: str, ctx: RequestContext) -> dict[str, Any]:
        vfs = get_viking_fs()
        internal_ctx = self._internal_ctx(ctx)
        try:
            data = json.loads(await vfs.read_file(_shared_meta_uri(upload_id), ctx=internal_ctx))
        except Exception as exc:
            raise PermissionDeniedError("Temporary upload metadata is invalid or missing.") from exc
        if not isinstance(data, dict):
            raise PermissionDeniedError("Temporary upload metadata is invalid or missing.")
        return data

    def _validate_shared_meta(
        self,
        meta: dict[str, Any],
        temp_file_id: str,
        ctx: RequestContext,
    ) -> None:
        if meta.get("temp_file_id") != temp_file_id:
            raise PermissionDeniedError("Invalid temp_file_id.")
        if meta.get("account") != ctx.account_id:
            raise PermissionDeniedError("Temporary upload does not belong to current account.")

    async def _cleanup_shared_uploads(self, ctx: RequestContext) -> tuple[int, int, int]:
        if self.temp_cfg.ttl_seconds == 0:
            return 0, 0, 0
        vfs = get_viking_fs()
        internal_ctx = self._internal_ctx(ctx)
        try:
            uploads = await vfs.ls(
                _SHARED_UPLOAD_ROOT,
                show_all_hidden=True,
                node_limit=LS_ALL_NODES,
                ctx=internal_ctx,
            )
        except Exception:
            logger.warning(
                "Shared temp upload cleanup list failed account=%s",
                ctx.account_id,
                exc_info=True,
            )
            raise

        now = time.time()
        cutoff = now - self.temp_cfg.ttl_seconds
        logger.debug(
            "Shared temp upload cleanup account=%s ttl_seconds=%s upload_count=%s "
            "now=%s cutoff=%s",
            ctx.account_id,
            self.temp_cfg.ttl_seconds,
            len(uploads),
            now,
            cutoff,
        )
        removed_count = 0
        failed_count = 0
        attempted_count = 0
        for upload in uploads:
            if not upload.get("isDir"):
                continue
            uri = str(upload.get("uri") or "").rstrip("/")
            upload_id = uri.removeprefix(f"{_SHARED_UPLOAD_ROOT}/")
            if not uri.startswith(f"{_SHARED_UPLOAD_ROOT}/") or "/" in upload_id:
                continue
            created_at = _shared_upload_created_at(upload_id)
            if created_at is None:
                logger.debug("Shared temp upload cleanup skipped malformed uri=%s", uri)
                continue
            age_seconds = now - created_at
            expired = created_at < cutoff
            logger.debug(
                "Shared temp upload cleanup candidate uri=%s created_at=%s "
                "age_seconds=%s expired=%s",
                uri,
                created_at,
                age_seconds,
                expired,
            )
            if not expired:
                continue
            attempted_count += 1
            try:
                await vfs.rm(uri, recursive=True, ctx=internal_ctx)
            except Exception:
                failed_count += 1
                logger.warning(
                    "Shared temp upload cleanup remove failed uri=%s",
                    uri,
                    exc_info=True,
                )
            else:
                removed_count += 1
                logger.debug("Shared temp upload cleanup removed uri=%s", uri)
            if attempted_count % _SHARED_CLEANUP_SLEEP_EVERY_REMOVALS == 0:
                await asyncio.sleep(_SHARED_CLEANUP_SLEEP_SECONDS)
        return (
            len(uploads),
            removed_count,
            failed_count,
        )

    def _cleanup_local_temp_files(self, temp_dir: Path) -> None:
        if self.temp_cfg.ttl_seconds == 0:
            return
        if not temp_dir.exists():
            return
        now = time.time()
        for file_path in temp_dir.iterdir():
            if not file_path.is_file():
                continue
            file_age = now - file_path.stat().st_mtime
            if file_age > self.temp_cfg.ttl_seconds:
                file_path.unlink(missing_ok=True)
                if not file_path.name.endswith(".ov_upload.meta"):
                    meta_path = temp_dir / f"{file_path.name}.ov_upload.meta"
                    if meta_path.exists():
                        meta_path.unlink(missing_ok=True)
