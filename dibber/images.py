import os
import time
from datetime import timedelta
from os.path import basename
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


def add_image_tag(image, uniq_id, tag, target_image=None):
    if target_image is None:
        target_image = image

    tag_cmd = ["docker", "tag", f"{image}:{uniq_id}", f"{target_image}:{tag}"]
    run(tag_cmd)


def remove_image_tag(image, tag):
    untag_cmd = ["docker", "rmi", f"{image}:{tag}"]
    run(untag_cmd)


def push_image(image, tag):
    push_cmd = ["docker", "push", f"{image}:{tag}"]
    run(push_cmd)


def get_build_contexts(contexts):
    build_contexts = []

    for context in contexts:
        image, sha256 = context.split(" ", maxsplit=1)
        base_image = basename(image)
        build_contexts += [
            "--build-context",
            f"{base_image}=docker-image://{image}@{sha256}",
        ]

    return build_contexts


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
    image: str, version: str, contexts: list[str] = [], local_only=True
) -> (str, str):
    start = time.perf_counter()

    # Need a temporary ID due to limitations of buildx
    uniq_id = make_id()

    config = get_config(image, version)
    name = f"{image}/{version}"
    repo = docker_image(image)
    tag = docker_tag(image, version)
    build_contexts = get_build_contexts(contexts)

    logger.info("Building {name}", name=name)

    # First build local image
    if local_only:
        cmd = ["docker", "build", name]
        cmd += ["-t", tag]
    else:
        cmd = ["docker", "buildx", "build", name]
        cmd += ["-t", f"{image}:{uniq_id}"]
        cmd += build_contexts

        cmd += ["--output", "type=docker"]
        cmd += ["--progress=plain"]

    full_cmd = " ".join(cmd)
    output = full_cmd + os.linesep
    output += run(cmd)
    output += os.linesep + os.linesep

    # Then push to registry, should be built already
    if not local_only:
        cmd = ["docker", "buildx", "build", name]
        cmd += ["-t", repo]
        cmd += build_contexts

        cmd += ["--progress=plain"]
        cmd += ["--output", "push-by-digest=true,type=image,push=true"]

        full_cmd = " ".join(cmd)
        output += full_cmd + os.linesep
        output += run(cmd)

    write_log(tag, output)

    # Find the sha256 tag for the image
    sha256 = ""
    for line in output.splitlines():
        if " exporting manifest " in line or " writing image " in line:
            for word in line.split(" "):
                if word.startswith("sha256:"):
                    sha256 = word.strip()
                    break
        if sha256 != "":
            break

    if sha256 == "":
        logger.error(output)
        raise Exception("Couldn't find sha256 tag in output")

    # Create tag map and additional local tags
    tag_map = [f"{tag} {sha256}"]
    if not local_only:
        tag_map = [f"{tag} {sha256}"]
        add_image_tag(image, uniq_id, version)
        for extra_tag in config.tags:
            full_name = docker_tag(image, extra_tag)
            tag_map += [f"{full_name} {sha256}"]

            add_image_tag(image, uniq_id, extra_tag)

        # Make sure we push the uniq ID tag to keep the image around
        add_image_tag(image, uniq_id, uniq_id, repo)
        push_image(repo, uniq_id)

        # Remove the now unnecessary unique ID
        remove_image_tag(image, uniq_id)
        remove_image_tag(repo, uniq_id)

    elapsed = time.perf_counter() - start
    logger.info(
        "Built and uploaded {name} in {elapsed}",
        name=name,
        elapsed=humanize.precisedelta(timedelta(seconds=elapsed)),
    )

    return tag_map, f"{repo}:{uniq_id}"


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
