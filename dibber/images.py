import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import humanize
from loguru import logger
from pydantic import BaseModel
from yaml import load

from dibber.settings import conf
from dibber.utils import make_id, run, write_log

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class Config(BaseModel):
    tags: List[str]


def find_images() -> Dict[str, List[str]]:
    result = {}

    images = [
        p.name for p in Path(".").iterdir() if p.is_dir() and not p.name.startswith(".")
    ]

    for image in images:
        versions = [
            p.name
            for p in Path(image).iterdir()
            if p.is_dir() and not p.name.startswith(".")
        ]
        result[image] = versions

    return result


class ImageConf:
    priority: int
    image: list[str]

    def __init__(self, priority: int, image: list[str]):
        self.priority = priority
        self.image = image

    def __repr__(self):
        return f"<{':'.join(self.image)} @ {self.priority} prio>"


def sort_images(images_: Dict[str, List[str]]) -> List[ImageConf]:
    images = []
    for image, versions in images_.items():
        for version in versions:
            images.append(f"{image}/{version}")
    images.sort()
    for image_or_list in conf.priority_builds:
        if isinstance(image_or_list, str):
            try:
                images.remove(image_or_list)
            except ValueError:
                logger.error(
                    "{image} found in PRIORITY_BUILDS is incorrect", image=image_or_list
                )
                raise
        else:
            for _img in image_or_list:
                try:
                    images.remove(_img)
                except ValueError:
                    logger.error(
                        "{image} found in PRIORITY_BUILDS is incorrect", image=_img
                    )
                    raise

    priority = 1
    result = []
    for image_or_list in conf.priority_builds:
        if isinstance(image_or_list, str):
            try:
                result.append(
                    ImageConf(
                        priority=priority, image=image_or_list.split("/", maxsplit=1)
                    )
                )
            except ValueError:
                logger.error(
                    "{image} found in PRIORITY_BUILDS is incorrect", image=image_or_list
                )
                raise
        else:
            for _img in image_or_list:
                try:
                    result.append(
                        ImageConf(priority=priority, image=_img.split("/", maxsplit=1))
                    )
                except ValueError:
                    logger.error(
                        "{image} found in PRIORITY_BUILDS is incorrect", image=_img
                    )
                    raise
        priority += 1

    result += [
        ImageConf(priority=priority, image=img.split("/", maxsplit=1)) for img in images
    ]

    return result


def add_image_tag(source_image_tag, destination_image_tag):
    tag_cmd = ["docker", "tag", source_image_tag, destination_image_tag]
    run(tag_cmd)


def remove_image_tag(image_tag):
    untag_cmd = ["regctl", "tag", "rm", image_tag]
    run(untag_cmd)


def push_image(image_tag):
    push_cmd = ["docker", "push", image_tag]
    run(push_cmd)


def get_build_contexts(contexts):
    build_contexts = []

    for context in contexts:
        local_tag, repo_uniq_id = context.split(" ", maxsplit=1)
        build_contexts += [
            "--build-context",
            f"{local_tag}=docker-image://{repo_uniq_id}",
        ]

    return build_contexts


def get_image_digest(image_tag) -> str:
    cmd = ["docker", "buildx", "imagetools", "inspect", image_tag]
    output = run(cmd)
    for line in output.splitlines():
        if line.startswith("Digest:"):
            return line[len("Digest:") :].strip()

    logger.error("Couldn't find sha256 digest for image")
    logger.error(output)
    sys.exit(1)


def inspect_manifest(image: str, digest: str):
    base_image = image.split(":", maxsplit=1)[0]

    cmd = ["docker", "manifest", "inspect", f"{base_image}@{digest}"]
    output = run(cmd)
    print(output)


def create_manifest(image: str, digests: list[str]):
    start = time.perf_counter()
    base_image = image.split(":", maxsplit=1)[0]

    cmd = ["docker", "buildx", "imagetools", "create", "-t", image]
    for digest in digests:
        cmd += [f"{base_image}@{digest}"]
    run(cmd)

    elapsed = time.perf_counter() - start
    logger.info(
        "Merged manifest for {image} in {elapsed}",
        image=image,
        elapsed=humanize.precisedelta(timedelta(seconds=elapsed)),
    )


def build_image(
    image: str,
    version: str,
    platform: str,
    contexts: list[str] = [],
    local_only=True,
) -> (str, str):
    start = time.perf_counter()

    # Need a temporary ID due to limitations of buildx
    uniq_id = make_id()

    config = get_config(image, version)
    dockerfile_path = f"{image}/{version}"
    repo_base = docker_image(image)
    repo_with_tag = docker_tag(image, version)
    repo_with_uniq_id = docker_tag(image, uniq_id)
    local_name = image
    local_with_tag = docker_tag(local_name, version, local=True)
    local_with_uniq_id = docker_tag(local_name, uniq_id, local=True)
    build_contexts = get_build_contexts(contexts)

    logger.info("Building {name}", name=dockerfile_path)

    # First build local image
    if local_only:
        cmd = ["docker", "build", dockerfile_path]
        cmd += ["-t", repo_with_tag]
        cmd += ["--platform", platform]
    else:
        cmd = ["docker", "buildx", "build", dockerfile_path]
        cmd += ["-t", local_with_uniq_id]
        cmd += ["--platform", platform]
        cmd += build_contexts

        cmd += ["--output", "type=docker"]
        cmd += ["--progress=plain"]

    full_cmd = " ".join(cmd)
    output = full_cmd + os.linesep
    output += run(cmd)
    output += os.linesep + os.linesep

    # Then push to registry, should be built already
    if not local_only:
        cmd = ["docker", "buildx", "build", dockerfile_path]
        cmd += ["-t", repo_base]  # Can't push with tag using push-by-digest
        cmd += ["--platform", platform]
        cmd += build_contexts

        cmd += ["--progress=plain"]
        cmd += ["--output", "push-by-digest=true,type=image,push=true"]

        full_cmd = " ".join(cmd)
        output += full_cmd + os.linesep
        output += run(cmd)

        # Also push the reference with uniq ID so this image is not lost
        add_image_tag(local_with_uniq_id, repo_with_uniq_id)
        push_image(repo_with_uniq_id)

    write_log(repo_with_tag, output)

    # Find the sha256 tag for the built image
    sha256 = get_image_digest(repo_with_tag if local_only else repo_with_uniq_id)

    # Create tag map and additional local tags
    tag_map = [f"{repo_with_tag} {sha256}"]
    if local_only:
        # ghcr.io/lietu/ubuntu-base:24.04 -> ubuntu-base:24.04
        add_image_tag(repo_with_tag, local_with_tag)
    else:
        # Add the proper target tags, for local and repo
        add_image_tag(repo_with_uniq_id, repo_with_tag)
        add_image_tag(repo_with_uniq_id, local_with_tag)

        # Add any additional tags
        for extra_tag in config.tags:
            extra_repo_tag = docker_tag(image, extra_tag)
            extra_local_tag = docker_tag(image, extra_tag, local=True)

            add_image_tag(repo_with_uniq_id, extra_repo_tag)
            add_image_tag(repo_with_uniq_id, extra_local_tag)

            tag_map += [f"{extra_repo_tag} {sha256}"]

        # Remove the now unnecessary unique ID locally
        # The pushed unique ID will be removed after manifest merging
        remove_image_tag(repo_with_uniq_id)

    elapsed = time.perf_counter() - start
    sha256_summary = sha256[:13] + "..." + sha256[-5:]

    logger.info(
        "Built and uploaded {name} as {repo_with_uniq_id} ({sha256}) in {elapsed}",
        name=dockerfile_path,
        repo_with_uniq_id=repo_with_uniq_id,
        sha256=sha256_summary,
        elapsed=humanize.precisedelta(timedelta(seconds=elapsed)),
    )

    return tag_map, f"{local_with_tag} {repo_with_uniq_id}", repo_with_uniq_id


def docker_image(image: str) -> str:
    return f"{conf.docker_user}/{image}"


def docker_tag(image: str, tag: str, local: bool = False) -> str:
    if not local:
        return f"{docker_image(image)}:{tag}"
    return f"{image}:{tag}"


def get_config(image: str, version: str) -> Config:
    config_path = f"{image}/{version}/config.yaml"
    config_text = Path(config_path).read_text(encoding="utf-8")
    config = load(config_text, Loader=Loader)
    return Config(**config)


def update_scanner():
    logger.info("Updating trivy database")
    run(["trivy", "image", "--download-db-only"])


def scan_image(image: str, version: str) -> bool:
    try:
        run(
            [
                "trivy",
                "image",
                "--skip-update",
                "--severity",
                "HIGH,CRITICAL",
                "--exit-code",
                "1",
                "--timeout",
                "7m",
                f"{docker_image(image)}:{version}",
            ],
            cwd=f"{image}/{version}",
        )
        return True
    except Exception:
        logger.error(
            "{image}:{version} has vulnerabilities!", image=image, version=version
        )
        return False
