import multiprocessing.context
import os
import signal
import sys
from enum import Enum
from multiprocessing import Pool
from typing import Optional

import click

import dibber.utils as utils
from dibber.images import (
    build_image,
    build_image_multiplatform,
    docker_tag,
    find_images,
    scan_image,
    sort_images,
    update_scanner,
    upload_tags_from_local_registry,
)
from dibber.settings import conf
from dibber.validation import validate


class Platform(str, Enum):
    LINUX_AMD64 = "linux/amd64"
    LINUX_ARM64 = "linux/arm64"


ALL_PLATFORMS = [Platform.LINUX_AMD64.value, Platform.LINUX_ARM64.value]


def init_pool(logger_, env):
    utils.logger = logger_
    os.environ.update(env)


def _build_images(pool, images, multiplatform, platform):
    if multiplatform:
        res = pool.starmap_async(
            build_image_multiplatform,
            [(image, version, ALL_PLATFORMS, False) for image, version in images],
        )
    else:
        res = pool.starmap_async(
            build_image,
            [(image, version, False, platform) for image, version in images],
        )

    while True:
        try:
            # Have a timeout to be non-blocking for signals
            res.get(0.25)
            break
        except multiprocessing.context.TimeoutError:
            pass


def _build_all_images(
    parallel: int, multiplatform: bool, platform: Optional[Platform] = None
):
    platform = platform.value if platform else None
    images = find_images()
    validate(images)
    sorted_images = sort_images(images)

    if parallel == 1:
        images = [img_conf.image for img_conf in sorted_images]
        for image, version in images:
            if multiplatform:
                build_image_multiplatform(image, version, ALL_PLATFORMS)
            else:
                build_image(image, version, platform=platform)
    else:
        utils.logger.info(f"Building {len(sorted_images)} images in {parallel} threads")
        utils.logger.remove()
        utils.logger.add(sys.stderr, enqueue=True, level="INFO")

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        with Pool(
            parallel, initializer=init_pool, initargs=(utils.logger, os.environ)
        ) as pool:
            signal.signal(signal.SIGINT, original_sigint_handler)

            max_prio = max(ic.priority for ic in sorted_images)
            for prio in range(1, max_prio + 1):
                images = [
                    img_conf.image
                    for img_conf in sorted_images
                    if img_conf.priority == prio
                ]
                try:
                    utils.logger.info(
                        "Building {c} priority {prio} images with up to {parallel} threads",
                        c=len(images),
                        prio=prio,
                        parallel=parallel,
                    )
                    _build_images(pool, images, multiplatform, platform)
                except KeyboardInterrupt:
                    utils.logger.error("Caught KeyboardInterrupt, terminating workers")
                    pool.terminate()
                    raise

            pool.close()


@click.group(help="Manage docker images")
def cli():
    print("foo")


@cli.command(help="Build docker images")
@click.option(
    "--parallel",
    default=2,
    type=int,
    help="Number of parallel images to build.",
    show_default=True,
)
@click.option(
    "--platform",
    default=None,
    type=Platform,
    help="Platform to build for.",
)
def build(parallel: int, platform: Optional[Platform]):
    _build_all_images(parallel, platform=platform, multiplatform=False)


@cli.command(help="Build docker images for multiple platforms")
@click.option(
    "--parallel",
    default=2,
    type=int,
    help="Number of parallel images to build.",
    show_default=True,
)
def build_multiplatform(
    parallel: int,
):
    _build_all_images(parallel, multiplatform=True)


@cli.command(help="Upload docker tags")
def upload():
    images = find_images()
    validate(images)
    upload_tags_from_local_registry(images)


@cli.command(help="Scan docker images")
def scan():
    update_scanner()
    images = find_images()
    vuln_images = []
    for image, versions in sorted(images.items()):
        for version in versions:
            if not scan_image(image, version):
                vuln_images.append(docker_tag(image, version))

    if vuln_images:
        utils.logger.error("Some images have vulnerabilities!")
        for img in vuln_images:
            print(f" - {img}")

        raise sys.exit(1)


@cli.command(help="List unique docker images managed by this tool")
def list():
    images = find_images()
    for image, versions in images.items():
        for version in versions:
            print(docker_tag(image, version))


@cli.command(help="Get the configured Docker username")
def docker_username():
    print(conf.docker_user)
