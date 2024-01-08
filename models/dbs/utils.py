from pathlib import Path


def delete_file(file_or_dir: Path, recursion: bool=False):
    try:
        while file_or_dir.exists():
            if file_or_dir.is_dir():
                file_or_dir.rmdir()
            else:
                file_or_dir.unlink()
            if not recursion:
                break
            file_or_dir = file_or_dir.parent
    except Exception:
        print(f'delete {file_or_dir} fail.')
