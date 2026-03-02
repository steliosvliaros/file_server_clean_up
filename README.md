# file_server_clean_up (SAFE, review-first)

This repo contains a **review-first** Jupyter notebook to audit a folder tree and produce a CSV of **cleanup candidates**.

## What it does (by default)
- Recursively scans a root folder
- Flags obvious junk files (e.g., `.DS_Store`, `Thumbs.db`, `desktop.ini`, temp/swap/bak files, zero-byte files)
- Detects **content duplicates** using hashing (size → fast head+tail fingerprint → optional full hash)
- Chooses **one** file to KEEP per duplicate group (configurable; default is newest modified time, with optional preferred-path bias)
- Writes a timestamped CSV report to `outputs/`

✅ **No deletion is performed by default.**

## Quick start (Windows 11 / Conda / Python 3.12)

Open **Anaconda Prompt**:

```bash
cd file_server_clean_up

conda create -n file_server_clean_up python=3.12 -y
conda activate file_server_clean_up

pip install jupyterlab pandas tqdm

jupyter lab
```

Then open:
- `notebooks/file_server_clean_up.ipynb`

Edit the configuration cell near the top:
- `ROOT = Path(r"CHANGE_ME")`

Run top-to-bottom. The report will be written to:
- `outputs/cleanup_candidates_YYYYMMDD_HHMMSS.csv`

## Optional quarantine move
The notebook includes an optional final cell that can **move** delete candidates into `quarantine/` (preserving relative paths).  
It is disabled by default: `ENABLE_QUARANTINE = False`.

⚠️ Review the CSV before enabling quarantine.

## Notes
- Network shares are supported (e.g., `\\SERVER\Share\Folder`), but hashing over a slow link may take time.
- Permissions errors are skipped; those files won't appear in the report unless they can be read/stat'ed.
