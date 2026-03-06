from __future__ import annotations

import csv
import hashlib
import os
import re
import stat
import shutil
import unicodedata
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


@dataclass(slots=True)
class NameNormalizationResult:
    plan_csv_path: Path
    reduction_csv_path: Path | None
    scanned_items: int
    planned_renames: int
    renamed_items: int
    failed_items: int
    over_limit_after_basic_normalization: int


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


_DATE_TOKEN_PATTERN = re.compile(
    r"(?<!\d)(\d{8}|\d{4}[.\-/ _]\d{1,2}[.\-/ _]\d{1,2}|\d{1,2}[.\-/ _]\d{1,2}[.\-/ _]\d{2,4})(?!\d)"
)


def _expand_two_digit_year(year_two_digits: int) -> int:
    # Common pivot for YY -> YYYY conversion.
    return 2000 + year_two_digits if year_two_digits <= 69 else 1900 + year_two_digits


def _parse_date_token(token: str) -> str | None:
    token = token.strip()
    try:
        if re.fullmatch(r"\d{8}", token):
            # Prefer YYYYMMDD when plausible, otherwise fallback to DDMMYYYY.
            y1, m1, d1 = int(token[0:4]), int(token[4:6]), int(token[6:8])
            try:
                return datetime(y1, m1, d1).strftime("%Y%m%d")
            except ValueError:
                d2, m2, y2 = int(token[0:2]), int(token[2:4]), int(token[4:8])
                return datetime(y2, m2, d2).strftime("%Y%m%d")

        if re.fullmatch(r"\d{4}[.\-/ _]\d{1,2}[.\-/ _]\d{1,2}", token):
            year, month, day = re.split(r"[.\-/ _]", token)
            return datetime(int(year), int(month), int(day)).strftime("%Y%m%d")

        if re.fullmatch(r"\d{1,2}[.\-/ _]\d{1,2}[.\-/ _]\d{2,4}", token):
            day, month, year = re.split(r"[.\-/ _]", token)
            parsed_year = int(year)
            if len(year) == 2:
                parsed_year = _expand_two_digit_year(parsed_year)
            return datetime(parsed_year, int(month), int(day)).strftime("%Y%m%d")
    except ValueError:
        return None

    return None


def _normalize_dates_in_name(name: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        formatted = _parse_date_token(token)
        if not formatted:
            return " "
        if match.start() == 0:
            return f"{formatted}_"
        if match.end() == len(name):
            return f"_{formatted}"
        return f"_{formatted}_"

    return _DATE_TOKEN_PATTERN.sub(_replace, name)


def _remove_special_characters(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    no_marks = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return "".join(ch if (ch.isalnum() or ch in {" ", "-", "_"}) else " " for ch in no_marks)


def _sanitize_component_name(name: str, *, is_file: bool) -> str:
    if is_file:
        stem = Path(name).stem
        extension = Path(name).suffix
    else:
        stem = name
        extension = ""

    stem = _normalize_dates_in_name(stem)
    stem = _remove_special_characters(stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    stem = stem.replace(" ", "-")
    stem = re.sub(r"_+", "_", stem)
    stem = re.sub(r"-+", "-", stem)
    stem = re.sub(r"(?:_-)|(?:-_)", "-", stem)
    stem = stem.strip("_-")

    if not stem:
        stem = "item"

    if not is_file:
        return stem

    ext_clean = extension.lower().strip()
    if ext_clean and not ext_clean.startswith("."):
        ext_clean = f".{ext_clean}"

    return f"{stem}{ext_clean}"


def _shorten_name_with_hash(name: str, *, is_file: bool, max_name_chars: int) -> str:
    if len(name) <= max_name_chars:
        return name

    digest = hashlib.blake2b(name.encode("utf-8"), digest_size=4).hexdigest()

    if not is_file:
        keep = max(1, max_name_chars - 9)
        return f"{name[:keep]}_{digest}"[:max_name_chars]

    stem = Path(name).stem
    ext = Path(name).suffix.lower()
    budget_for_stem = max(1, max_name_chars - len(ext) - 9)
    short_stem = stem[:budget_for_stem]
    candidate = f"{short_stem}_{digest}{ext}"
    if len(candidate) <= max_name_chars:
        return candidate
    # Last resort: trim extension if the original extension is unusually long.
    return candidate[:max_name_chars]


def _build_unique_target_name(parent: Path, desired_name: str) -> Path:
    candidate = parent / desired_name
    if not candidate.exists():
        return candidate

    if candidate.is_dir():
        base_name = desired_name
        suffix = ""
    else:
        base_name = Path(desired_name).stem
        suffix = Path(desired_name).suffix

    counter = 2
    while True:
        next_name = f"{base_name}-{counter:02d}{suffix}"
        candidate = parent / next_name
        if not candidate.exists():
            return candidate
        counter += 1


def _is_reparse_or_link_dir(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        st = path.lstat()
        if hasattr(st, "st_file_attributes"):
            return bool(st.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)
    except OSError:
        return True
    return False


def _fit_name_to_max_path(
    *,
    parent: Path,
    desired_name: str,
    is_file: bool,
    max_safe_path_chars: int,
) -> tuple[str, Path, int, bool, bool]:
    proposed_path = parent / desired_name
    proposed_len = len(str(proposed_path))
    if proposed_len <= max_safe_path_chars:
        return desired_name, proposed_path, proposed_len, False, True

    allowed_name_len = max_safe_path_chars - len(str(parent)) - 1
    if allowed_name_len < 1:
        forced_name = "x"
        if is_file:
            ext = Path(desired_name).suffix.lower()
            if ext and len(ext) < max_safe_path_chars:
                forced_name = f"x{ext}"
        forced_path = parent / forced_name
        return forced_name, forced_path, len(str(forced_path)), forced_name != desired_name, False

    fitted_name = _shorten_name_with_hash(
        desired_name,
        is_file=is_file,
        max_name_chars=allowed_name_len,
    )
    if len(fitted_name) > allowed_name_len:
        fitted_name = fitted_name[:allowed_name_len]

    fitted_path = parent / fitted_name
    fitted_len = len(str(fitted_path))
    if fitted_len <= max_safe_path_chars:
        return fitted_name, fitted_path, fitted_len, fitted_name != desired_name, True

    # Final aggressive fallback trimming by single characters.
    if is_file:
        ext = Path(fitted_name).suffix.lower()
        stem = Path(fitted_name).stem
        candidate_name = f"{stem}{ext}"
        while stem and len(str(parent / candidate_name)) > max_safe_path_chars:
            stem = stem[:-1]
            candidate_name = f"{stem or 'x'}{ext}"
    else:
        stem = fitted_name
        while stem and len(str(parent / stem)) > max_safe_path_chars:
            stem = stem[:-1]
        candidate_name = stem or "x"

    candidate_path = parent / candidate_name
    candidate_len = len(str(candidate_path))
    return candidate_name, candidate_path, candidate_len, candidate_name != desired_name, candidate_len <= max_safe_path_chars


def normalize_names_and_export_reduction_csv(
    root_path: str | Path,
    output_plan_csv_path: str | Path,
    *,
    max_safe_path_chars: int = 240,
    dry_run: bool = True,
    dry_run_minimal_csv: bool = True,
    write_reduction_csv: bool = True,
    skip_hidden_dirs: bool = False,
    skip_underscore_dirs: bool = True,
    skip_reparse_dirs: bool = True,
    reduction_csv_path: str | Path | None = None,
) -> NameNormalizationResult:
    root = Path(root_path)
    plan_csv = Path(output_plan_csv_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid root path: {root}")
    if max_safe_path_chars < 1:
        raise ValueError("max_safe_path_chars must be >= 1")

    reduction_csv: Path | None
    if write_reduction_csv:
        if reduction_csv_path is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            reduction_csv = plan_csv.with_name(f"name_reduction_proposals_{ts}.csv")
        else:
            reduction_csv = Path(reduction_csv_path)
    else:
        reduction_csv = None

    plan_csv.parent.mkdir(parents=True, exist_ok=True)
    if reduction_csv is not None:
        reduction_csv.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    reduction_rows: list[dict] = []

    scanned_items = 0
    planned_renames = 0
    renamed_items = 0
    failed_items = 0
    over_limit_after_basic_normalization = 0

    if dry_run and dry_run_minimal_csv:
        plan_columns = [
            "item_type",
            "current_path",
            "current_name",
            "proposed_name",
            "proposed_path_final",
            "path_char_count_initial",
            "path_char_count_final",
            "is_within_safe_limit",
        ]
        reduction_columns = [
            "item_type",
            "current_path",
            "current_name",
            "proposed_name",
            "proposed_path",
            "path_char_count_initial",
            "path_char_count_final",
            "is_over_safe_limit",
        ]

        with plan_csv.open("w", newline="", encoding="utf-8-sig") as plan_stream:
            plan_writer = csv.DictWriter(plan_stream, fieldnames=plan_columns)
            plan_writer.writeheader()

            reduction_stream = None
            reduction_writer = None
            try:
                if reduction_csv is not None:
                    reduction_stream = reduction_csv.open("w", newline="", encoding="utf-8-sig")
                    reduction_writer = csv.DictWriter(reduction_stream, fieldnames=reduction_columns)
                    reduction_writer.writeheader()

                for current_dir, dirs, files in os.walk(root, topdown=True):
                    if skip_hidden_dirs:
                        dirs[:] = [name for name in dirs if not name.startswith(".")]
                    if skip_underscore_dirs:
                        dirs[:] = [name for name in dirs if not name.startswith("_")]
                    if skip_reparse_dirs:
                        current_path_for_dirs = Path(current_dir)
                        dirs[:] = [
                            name
                            for name in dirs
                            if not _is_reparse_or_link_dir(current_path_for_dirs / name)
                        ]

                    current_path = Path(current_dir)

                    for dir_name in dirs:
                        scanned_items += 1
                        source_dir_path = current_path / dir_name
                        basic_name = _sanitize_component_name(dir_name, is_file=False)
                        (
                            proposed_name,
                            proposed_path,
                            proposed_len,
                            _reduction_applied,
                            fits_limit,
                        ) = _fit_name_to_max_path(
                            parent=current_path,
                            desired_name=basic_name,
                            is_file=False,
                            max_safe_path_chars=max_safe_path_chars,
                        )
                        source_len = len(str(source_dir_path))
                        if dir_name != proposed_name:
                            planned_renames += 1

                        plan_writer.writerow(
                            {
                                "item_type": "directory",
                                "current_path": str(source_dir_path),
                                "current_name": dir_name,
                                "proposed_name": proposed_name,
                                "proposed_path_final": str(proposed_path),
                                "path_char_count_initial": source_len,
                                "path_char_count_final": proposed_len,
                                "is_within_safe_limit": fits_limit,
                            }
                        )

                        if reduction_writer is not None and not fits_limit:
                            over_limit_after_basic_normalization += 1
                            reduction_writer.writerow(
                                {
                                    "item_type": "directory",
                                    "current_path": str(source_dir_path),
                                    "current_name": dir_name,
                                    "proposed_name": proposed_name,
                                    "proposed_path": str(proposed_path),
                                    "path_char_count_initial": source_len,
                                    "path_char_count_final": proposed_len,
                                    "is_over_safe_limit": True,
                                }
                            )

                    for file_name in files:
                        scanned_items += 1
                        basic_name = _sanitize_component_name(file_name, is_file=True)
                        source_path = current_path / file_name
                        (
                            proposed_name,
                            proposed_path,
                            proposed_len,
                            _reduction_applied,
                            fits_limit,
                        ) = _fit_name_to_max_path(
                            parent=current_path,
                            desired_name=basic_name,
                            is_file=True,
                            max_safe_path_chars=max_safe_path_chars,
                        )
                        source_len = len(str(source_path))
                        if file_name != proposed_name:
                            planned_renames += 1

                        plan_writer.writerow(
                            {
                                "item_type": "file",
                                "current_path": str(source_path),
                                "current_name": file_name,
                                "proposed_name": proposed_name,
                                "proposed_path_final": str(proposed_path),
                                "path_char_count_initial": source_len,
                                "path_char_count_final": proposed_len,
                                "is_within_safe_limit": fits_limit,
                            }
                        )

                        if reduction_writer is not None:
                            if not fits_limit:
                                over_limit_after_basic_normalization += 1
                                reduction_writer.writerow(
                                    {
                                        "item_type": "file",
                                        "current_path": str(source_path),
                                        "current_name": file_name,
                                        "proposed_name": proposed_name,
                                        "proposed_path": str(proposed_path),
                                        "path_char_count_initial": source_len,
                                        "path_char_count_final": proposed_len,
                                        "is_over_safe_limit": True,
                                    }
                                )
            finally:
                if reduction_stream is not None:
                    reduction_stream.close()

        LOGGER.info(
            "Name normalization complete",
            extra={
                "root_path": str(root),
                "plan_csv_path": str(plan_csv),
                "reduction_csv_path": str(reduction_csv) if reduction_csv is not None else "",
                "scanned_items": scanned_items,
                "planned_renames": planned_renames,
                "renamed_items": 0,
                "failed_items": 0,
                "over_limit_after_basic_normalization": over_limit_after_basic_normalization,
                "write_reduction_csv": write_reduction_csv,
                "dry_run": True,
                "dry_run_minimal_csv": True,
            },
        )

        return NameNormalizationResult(
            plan_csv_path=plan_csv,
            reduction_csv_path=reduction_csv,
            scanned_items=scanned_items,
            planned_renames=planned_renames,
            renamed_items=0,
            failed_items=0,
            over_limit_after_basic_normalization=over_limit_after_basic_normalization,
        )
    else:
        plan_columns = [
            "item_type",
            "current_path",
            "current_name",
            "proposed_name_basic",
            "proposed_name_final",
            "proposed_path_final",
            "path_char_count_initial",
            "path_char_count_final",
            "is_over_safe_limit_after_basic",
            "is_over_safe_limit_after_final",
            "reduction_applied",
            "needs_rename",
            "status",
        ]
        reduction_columns = plan_columns

        directories_to_process: list[Path] = []

        for current_dir, dirs, files in os.walk(root, topdown=True):
            if skip_hidden_dirs:
                dirs[:] = [name for name in dirs if not name.startswith(".")]
            if skip_underscore_dirs:
                dirs[:] = [name for name in dirs if not name.startswith("_")]
            if skip_reparse_dirs:
                current_path_for_dirs = Path(current_dir)
                dirs[:] = [
                    name
                    for name in dirs
                    if not _is_reparse_or_link_dir(current_path_for_dirs / name)
                ]

            current_path = Path(current_dir)
            directories_to_process.append(current_path)

            for file_name in files:
                file_path = current_path / file_name
                try:
                    if not file_path.is_file() or file_path.is_symlink():
                        continue
                except OSError:
                    continue

                scanned_items += 1
                basic_name = _sanitize_component_name(file_name, is_file=True)
                basic_target = current_path / basic_name
                basic_target_len = len(str(basic_target))
                over_limit_basic = basic_target_len > max_safe_path_chars
                if over_limit_basic:
                    over_limit_after_basic_normalization += 1

                final_name, final_target, final_target_len, reduction_applied, fits_limit = _fit_name_to_max_path(
                    parent=current_path,
                    desired_name=basic_name,
                    is_file=True,
                    max_safe_path_chars=max_safe_path_chars,
                )
                over_limit_final = not fits_limit

                needs_rename = file_name != final_name
                if needs_rename:
                    planned_renames += 1

                status = "PLANNED"
                if needs_rename and not dry_run:
                    try:
                        target = _build_unique_target_name(current_path, final_name)
                        file_path.rename(target)
                        final_target = target
                        final_target_len = len(str(final_target))
                        over_limit_final = final_target_len > max_safe_path_chars
                        renamed_items += 1
                        status = "RENAMED"
                    except Exception as exc:
                        failed_items += 1
                        status = f"FAILED: {exc}"

                row = {
                    "item_type": "file",
                    "current_path": str(file_path),
                    "current_name": file_name,
                    "proposed_name_basic": basic_name,
                    "proposed_name_final": final_name,
                    "proposed_path_final": str(final_target),
                    "path_char_count_initial": len(str(file_path)),
                    "is_over_safe_limit_after_basic": over_limit_basic,
                    "is_over_safe_limit_after_final": over_limit_final,
                    "reduction_applied": reduction_applied,
                    "path_char_count_final": final_target_len,
                    "needs_rename": needs_rename,
                    "status": status,
                }
                rows.append(row)

                if over_limit_basic:
                    reduction_rows.append(row)

        # Rename directories from deepest to shallowest so children are handled first.
        for current_path in sorted(directories_to_process, key=lambda p: len(p.parts), reverse=True):
            if current_path == root:
                continue

            scanned_items += 1
            parent = current_path.parent
            current_name = current_path.name
            basic_name = _sanitize_component_name(current_name, is_file=False)
            basic_target = parent / basic_name
            basic_target_len = len(str(basic_target))
            over_limit_basic = basic_target_len > max_safe_path_chars
            if over_limit_basic:
                over_limit_after_basic_normalization += 1

            final_name, final_target, final_target_len, reduction_applied, fits_limit = _fit_name_to_max_path(
                parent=parent,
                desired_name=basic_name,
                is_file=False,
                max_safe_path_chars=max_safe_path_chars,
            )
            over_limit_final = not fits_limit

            needs_rename = current_name != final_name
            if needs_rename:
                planned_renames += 1

            status = "PLANNED"
            if needs_rename and not dry_run:
                try:
                    target = _build_unique_target_name(parent, final_name)
                    current_path.rename(target)
                    final_target = target
                    final_target_len = len(str(final_target))
                    over_limit_final = final_target_len > max_safe_path_chars
                    renamed_items += 1
                    status = "RENAMED"
                except Exception as exc:
                    failed_items += 1
                    status = f"FAILED: {exc}"

            row = {
                "item_type": "directory",
                "current_path": str(current_path),
                "current_name": current_name,
                "proposed_name_basic": basic_name,
                "proposed_name_final": final_name,
                "proposed_path_final": str(final_target),
                "path_char_count_initial": len(str(current_path)),
                "is_over_safe_limit_after_basic": over_limit_basic,
                "is_over_safe_limit_after_final": over_limit_final,
                "reduction_applied": reduction_applied,
                "path_char_count_final": final_target_len,
                "needs_rename": needs_rename,
                "status": status,
            }
            rows.append(row)

            if over_limit_basic:
                reduction_rows.append(row)

    with plan_csv.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=plan_columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in plan_columns})

    if reduction_csv is not None:
        with reduction_csv.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(stream, fieldnames=reduction_columns)
            writer.writeheader()
            for row in reduction_rows:
                writer.writerow({column: row.get(column, "") for column in reduction_columns})

    LOGGER.info(
        "Name normalization complete",
        extra={
            "root_path": str(root),
            "plan_csv_path": str(plan_csv),
            "reduction_csv_path": str(reduction_csv) if reduction_csv is not None else "",
            "scanned_items": scanned_items,
            "planned_renames": planned_renames,
            "renamed_items": renamed_items,
            "failed_items": failed_items,
            "over_limit_after_basic_normalization": over_limit_after_basic_normalization,
                "write_reduction_csv": write_reduction_csv,
            "dry_run": dry_run,
        },
    )

    return NameNormalizationResult(
        plan_csv_path=plan_csv,
        reduction_csv_path=reduction_csv,
        scanned_items=scanned_items,
        planned_renames=planned_renames,
        renamed_items=renamed_items,
        failed_items=failed_items,
        over_limit_after_basic_normalization=over_limit_after_basic_normalization,
    )


def export_file_inventory_csv(
    root_path: str | Path,
    output_csv_path: str | Path,
    *,
    skip_hidden_dirs: bool = False,
    skip_underscore_dirs: bool = True,
    hash_algorithm: str = "blake2b",
    compute_content_duplicates: bool = False,
    allowed_extensions: list[str] | set[str] | tuple[str, ...] | None = None,
    max_safe_path_chars: int = 240,
) -> FileInventoryResult:
    root = Path(root_path)
    output_csv = Path(output_csv_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid root path: {root}")
    if max_safe_path_chars < 1:
        raise ValueError("max_safe_path_chars must be >= 1")

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
        if skip_underscore_dirs:
            dirs[:] = [name for name in dirs if not name.startswith("_")]

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
                    "path_char_count": len(str(file_path)),
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

        record["is_path_at_or_over_safe_limit"] = record["path_char_count"] >= max_safe_path_chars
        if record["is_path_at_or_over_safe_limit"]:
            reasons.append("path_at_or_over_safe_limit")

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
        "path_char_count",
        "is_path_at_or_over_safe_limit",
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
