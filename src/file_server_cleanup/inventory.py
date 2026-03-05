from __future__ import annotations

import csv
import hashlib
import os
import shutil
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .logging_config import get_logger

LOGGER = get_logger(__name__)


@dataclass(slots=True)
class FileInventoryResult:
    csv_path: Path
    scanned_files: int
    duplicate_name_ext_created_files: int
    duplicate_content_files: int
    unique_file_types: tuple[str, ...]


@dataclass(slots=True)
class MoveDuplicatesResult:
    csv_path: Path
    duplicated_folder: Path
    duplicate_rows: int
    planned_moves: int
    moved_files: int
    skipped_files: int
    failed_files: int


@dataclass(slots=True)
class MoveDeprecatedResult:
    csv_path: Path
    deprecated_folder: Path
    deprecated_rows: int
    planned_moves: int
    moved_files: int
    skipped_files: int
    failed_files: int


@dataclass(slots=True)
class EmptyFoldersCleanupResult:
    root_path: Path
    inspected_dirs: int
    deleted_dirs: int
    skipped_dirs: int
    failed_dirs: int
    dry_run: bool


def _safe_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")


def _safe_owner(path: Path) -> str:
    try:
        return path.owner()
    except Exception:
        return "unknown"


def _to_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.min


def _hash_file(path: Path, algorithm: str = "blake2b", chunk_size: int = 8 * 1024 * 1024) -> str:
    if algorithm == "blake2b":
        hasher = hashlib.blake2b(digest_size=32)
    elif algorithm == "sha256":
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with path.open("rb") as stream:
        while True:
            block = stream.read(chunk_size)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def export_file_inventory_csv(
    root_path: str | Path,
    output_csv_path: str | Path,
    *,
    skip_hidden_dirs: bool = False,
    hash_algorithm: str = "blake2b",
    compute_content_duplicates: bool = False,
    allowed_extensions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> FileInventoryResult:
    root = Path(root_path)
    output_csv = Path(output_csv_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid root path: {root}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    normalized_allowed_extensions: set[str] | None = None
    if allowed_extensions is not None:
        normalized_allowed_extensions = {
            ext.lower() if str(ext).startswith(".") else f".{str(ext).lower()}"
            for ext in allowed_extensions
            if str(ext).strip()
        }

    records: list[dict] = []

    LOGGER.info("Starting inventory scan", extra={"root": str(root)})

    for current_dir, dirs, files in os.walk(root):
        if skip_hidden_dirs:
            dirs[:] = [name for name in dirs if not name.startswith(".")]

        current = Path(current_dir)

        for file_name in files:
            file_path = current / file_name
            try:
                if not file_path.is_file() or file_path.is_symlink():
                    continue
                stat = file_path.stat()
            except (FileNotFoundError, PermissionError, OSError):
                continue

            records.append(
                {
                    "file_name": file_path.stem,
                    "extension": file_path.suffix.lower(),
                    "path": str(file_path),
                    "created_at": _safe_iso(stat.st_ctime),
                    "created_date": datetime.fromtimestamp(stat.st_ctime).date().isoformat(),
                    "modified_at": _safe_iso(stat.st_mtime),
                    "size_bytes": int(stat.st_size),
                    "owner": _safe_owner(file_path),
                    "name_ext_key": f"{file_path.stem.lower()}|{file_path.suffix.lower()}",
                    "size_key": str(int(stat.st_size)),
                    "content_hash": "",
                }
            )

    name_ext_counts = Counter(record["name_ext_key"] for record in records)
    size_counts = Counter(record["size_key"] for record in records)

    if compute_content_duplicates:
        size_duplicates = {size for size, count in size_counts.items() if count > 1}

        for record in records:
            if record["size_key"] not in size_duplicates:
                continue
            file_path = Path(record["path"])
            try:
                record["content_hash"] = _hash_file(file_path, algorithm=hash_algorithm)
            except (FileNotFoundError, PermissionError, OSError):
                record["content_hash"] = ""

    content_counts = Counter(record["content_hash"] for record in records if record["content_hash"])

    for record in records:
        duplicate_name_ext_count = name_ext_counts[record["name_ext_key"]]
        duplicate_size_count = size_counts[record["size_key"]]
        duplicate_content_count = content_counts[record["content_hash"]] if record["content_hash"] else 0

        reasons: list[str] = []
        is_dup_name_ext = duplicate_name_ext_count > 1
        is_dup_content = duplicate_content_count > 1

        if is_dup_name_ext:
            reasons.append("same_name_extension")

        if is_dup_content:
            reasons.append("same_content_hash")

        if not compute_content_duplicates:
            reasons.append("content_check_not_computed")

        extension = record["extension"]
        if normalized_allowed_extensions is None:
            record["is_extension_not_allowed"] = False
        else:
            record["is_extension_not_allowed"] = extension not in normalized_allowed_extensions
            if record["is_extension_not_allowed"]:
                reasons.append("extension_not_allowed")

        record["duplicated"] = is_dup_name_ext or is_dup_content
        record["flag_reason"] = ";".join(reasons)
        record["is_duplicate_name_ext_created"] = is_dup_name_ext
        record["is_duplicate_size"] = duplicate_size_count > 1
        record["is_duplicate_content"] = is_dup_content

    columns = [
        "file_name",
        "extension",
        "path",
        "created_at",
        "created_date",
        "modified_at",
        "size_bytes",
        "owner",
        "is_extension_not_allowed",
        "duplicated",
        "flag_reason",
        "content_hash",
    ]

    with output_csv.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow({column: record.get(column, "") for column in columns})

    duplicate_name_ext_created_files = sum(1 for record in records if record["is_duplicate_name_ext_created"])
    duplicate_content_files = sum(1 for record in records if record["is_duplicate_content"])
    unique_file_types = tuple(sorted({record["extension"] for record in records if record["extension"]}))

    LOGGER.info(
        "Inventory export complete",
        extra={
            "csv_path": str(output_csv),
            "scanned_files": len(records),
            "duplicate_name_ext_created_files": duplicate_name_ext_created_files,
            "duplicate_content_files": duplicate_content_files,
        },
    )

    return FileInventoryResult(
        csv_path=output_csv,
        scanned_files=len(records),
        duplicate_name_ext_created_files=duplicate_name_ext_created_files,
        duplicate_content_files=duplicate_content_files,
        unique_file_types=unique_file_types,
    )


def move_duplicated_files_from_csv(
    scan_root: str | Path,
    inventory_csv_path: str | Path,
    *,
    duplicated_folder_name: str = "_DUPLICATED",
    dry_run: bool = True,
) -> MoveDuplicatesResult:
    root = Path(scan_root)
    csv_path = Path(inventory_csv_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid scan root: {root}")
    if not csv_path.exists() or not csv_path.is_file():
        raise FileNotFoundError(f"Inventory CSV not found: {csv_path}")

    duplicated_root = root / duplicated_folder_name
    if not dry_run:
        duplicated_root.mkdir(parents=True, exist_ok=True)

    duplicate_rows = 0
    planned_moves = 0
    moved_files = 0
    skipped_files = 0
    failed_files = 0

    duplicated_candidates: list[dict] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            if not _to_bool(row.get("duplicated")):
                continue
            duplicate_rows += 1
            duplicated_candidates.append(row)

    grouped_candidates: dict[tuple[str, str], list[dict]] = {}
    for row in duplicated_candidates:
        source_path_raw = (row.get("path") or "").strip()
        source_path = Path(source_path_raw) if source_path_raw else None
        file_name = (row.get("file_name") or (source_path.stem if source_path else "")).strip().lower()
        extension = (row.get("extension") or (source_path.suffix if source_path else "")).strip().lower()
        key = (file_name, extension)
        grouped_candidates.setdefault(key, []).append(row)

    keeper_paths: set[str] = set()
    for group_rows in grouped_candidates.values():
        if not group_rows:
            continue
        keeper = max(
            group_rows,
            key=lambda row: (
                _parse_iso_datetime(row.get("created_at")),
                _parse_iso_datetime(row.get("modified_at")),
                (row.get("path") or "").strip().lower(),
            ),
        )
        keeper_path = (keeper.get("path") or "").strip()
        if keeper_path:
            keeper_paths.add(keeper_path)

    for row in duplicated_candidates:
        source_path_raw = (row.get("path") or "").strip()
        if not source_path_raw:
            skipped_files += 1
            continue

        if source_path_raw in keeper_paths:
            skipped_files += 1
            continue

        source = Path(source_path_raw)
        if not source.exists() or not source.is_file():
            skipped_files += 1
            continue

        try:
            relative_source = source.relative_to(root)
        except ValueError:
            skipped_files += 1
            continue

        if relative_source.parts and relative_source.parts[0] == duplicated_folder_name:
            skipped_files += 1
            continue

        target = duplicated_root / root.name / relative_source
        suffix_counter = 2
        while target.exists():
            target = target.with_name(f"{target.stem}-{suffix_counter:02d}{target.suffix}")
            suffix_counter += 1

        planned_moves += 1

        if dry_run:
            continue

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(target))
            moved_files += 1
        except Exception:
            failed_files += 1

    LOGGER.info(
        "Duplicate move operation complete",
        extra={
            "csv_path": str(csv_path),
            "duplicated_folder": str(duplicated_root),
            "duplicate_rows": duplicate_rows,
            "planned_moves": planned_moves,
            "moved_files": moved_files,
            "skipped_files": skipped_files,
            "failed_files": failed_files,
            "dry_run": dry_run,
        },
    )

    return MoveDuplicatesResult(
        csv_path=csv_path,
        duplicated_folder=duplicated_root,
        duplicate_rows=duplicate_rows,
        planned_moves=planned_moves,
        moved_files=moved_files,
        skipped_files=skipped_files,
        failed_files=failed_files,
    )


def move_deprecated_files_from_csv(
    scan_root: str | Path,
    inventory_csv_path: str | Path,
    *,
    deprecated_folder_name: str = "_DEPRECATED",
    dry_run: bool = True,
) -> MoveDeprecatedResult:
    root = Path(scan_root)
    csv_path = Path(inventory_csv_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid scan root: {root}")
    if not csv_path.exists() or not csv_path.is_file():
        raise FileNotFoundError(f"Inventory CSV not found: {csv_path}")

    deprecated_root = root / deprecated_folder_name
    if not dry_run:
        deprecated_root.mkdir(parents=True, exist_ok=True)

    deprecated_rows = 0
    planned_moves = 0
    moved_files = 0
    skipped_files = 0
    failed_files = 0

    with csv_path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            if not _to_bool(row.get("is_extension_not_allowed")):
                continue

            deprecated_rows += 1

            source_path_raw = (row.get("path") or "").strip()
            if not source_path_raw:
                skipped_files += 1
                continue

            source = Path(source_path_raw)
            if not source.exists() or not source.is_file():
                skipped_files += 1
                continue

            try:
                relative_source = source.relative_to(root)
            except ValueError:
                skipped_files += 1
                continue

            if relative_source.parts and relative_source.parts[0] == deprecated_folder_name:
                skipped_files += 1
                continue

            target = deprecated_root / root.name / relative_source
            suffix_counter = 2
            while target.exists():
                target = target.with_name(f"{target.stem}-{suffix_counter:02d}{target.suffix}")
                suffix_counter += 1

            planned_moves += 1

            if dry_run:
                continue

            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source), str(target))
                moved_files += 1
            except Exception:
                failed_files += 1

    LOGGER.info(
        "Deprecated move operation complete",
        extra={
            "csv_path": str(csv_path),
            "deprecated_folder": str(deprecated_root),
            "deprecated_rows": deprecated_rows,
            "planned_moves": planned_moves,
            "moved_files": moved_files,
            "skipped_files": skipped_files,
            "failed_files": failed_files,
            "dry_run": dry_run,
        },
    )

    return MoveDeprecatedResult(
        csv_path=csv_path,
        deprecated_folder=deprecated_root,
        deprecated_rows=deprecated_rows,
        planned_moves=planned_moves,
        moved_files=moved_files,
        skipped_files=skipped_files,
        failed_files=failed_files,
    )


def delete_empty_folders(
    root_path: str | Path,
    *,
    remove_root: bool = False,
    dry_run: bool = True,
    exclude_dir_names: list[str] | set[str] | tuple[str, ...] | None = None,
) -> EmptyFoldersCleanupResult:
    root = Path(root_path)
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid root path: {root}")

    normalized_excluded = {name.strip() for name in (exclude_dir_names or []) if str(name).strip()}

    inspected_dirs = 0
    deleted_dirs = 0
    skipped_dirs = 0
    failed_dirs = 0

    for current_dir, _, _ in os.walk(root, topdown=False):
        current_path = Path(current_dir)
        inspected_dirs += 1

        if current_path == root and not remove_root:
            skipped_dirs += 1
            continue

        if current_path.name in normalized_excluded:
            skipped_dirs += 1
            continue

        try:
            if any(current_path.iterdir()):
                continue
        except OSError:
            failed_dirs += 1
            continue

        if dry_run:
            deleted_dirs += 1
            continue

        try:
            current_path.rmdir()
            deleted_dirs += 1
        except OSError:
            failed_dirs += 1

    LOGGER.info(
        "Empty folder cleanup complete",
        extra={
            "root_path": str(root),
            "inspected_dirs": inspected_dirs,
            "deleted_dirs": deleted_dirs,
            "skipped_dirs": skipped_dirs,
            "failed_dirs": failed_dirs,
            "dry_run": dry_run,
            "remove_root": remove_root,
        },
    )

    return EmptyFoldersCleanupResult(
        root_path=root,
        inspected_dirs=inspected_dirs,
        deleted_dirs=deleted_dirs,
        skipped_dirs=skipped_dirs,
        failed_dirs=failed_dirs,
        dry_run=dry_run,
    )
