import os
import secrets
import string
from pathlib import Path
from subprocess import PIPE  # nosec
from subprocess import run as sp_run  # nosec
from typing import List

from loguru import logger
from pathvalidate import sanitize_filename

_ = logger
LOG_DIR = Path("logs")


def make_id() -> str:
    return "".join(
        secrets.choice(string.ascii_uppercase + string.digits) for _ in range(10)
    )


def sanitize(filename: str) -> str:
    return sanitize_filename(filename.replace("/", "__"))


def write_log(tag_name, contents):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / (sanitize(tag_name) + ".log")
    log_file.write_text(contents)


def run(cmd: List[str], cwd=None) -> str:
    logger.debug("Executing: {cmd}", cmd=" ".join(cmd))
    result = sp_run(cmd, stdout=PIPE, stderr=PIPE, cwd=cwd)  # nosec B603

    chunks = [" ----- STDOUT ----- "]
    chunks += [result.stdout.decode("utf-8")]
    chunks += [" ----- STDERR ----- "]
    chunks += [result.stderr.decode("utf-8")]
    chunks += [" ----- END ----- "]
    output = os.linesep.join(chunks)

    if result.returncode > 0:
        logger.error(
            "{cmd} exited with code {code}",
            cmd=" ".join(cmd),
            code=result.returncode,
        )
        logger.error(output)
        raise Exception(f"Failed to run {cmd}")

    return output
