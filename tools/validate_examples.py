import logging
import os
import subprocess  # nosec
import sys
import time

from rich.logging import RichHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)

EXAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "examples")
EXAMPLE_FILES = [
    os.path.join(EXAMPLES_DIR, file)
    for file in os.listdir(EXAMPLES_DIR)
    if file.endswith(".py")
]

logging.info(f"Found {len(EXAMPLE_FILES)} example files in {EXAMPLES_DIR}.")


def run_single_example(file: str) -> int:
    p = subprocess.Popen(  # nosec
        ["python", file],
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    time.sleep(5)
    p.terminate()
    output, error = p.communicate()  # needed for returncode to be set
    return p.returncode


def run_all_examples(example_files: list[str]) -> None:
    error_flag = False
    for file in example_files:
        short_name = os.path.basename(file)
        status_code = run_single_example(file)
        # look for status code 1 (not 0) because we will not necessarily get 0 due to calling subprocess.Popen.terminate
        if status_code == 1:
            logging.error(f"❌ {short_name}")
            error_flag = True
        else:
            logging.info(f"✅ {short_name}")
    if error_flag:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    run_all_examples(EXAMPLE_FILES)
