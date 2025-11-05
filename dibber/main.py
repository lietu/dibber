import multiprocessing.context
import os
import platform as platformlib
import signal
import sys
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import click

import dibber.utils as utils
from dibber.images import (
    build_image,
    create_manifest,
    docker_tag,
    find_images,
    scan_image,
    sort_images,
    update_scanner,
)
from dibber.settings import conf
from dibber.validation import validate


def init_pool(logger_, env):
    utils.logger = logger_
    os.environ.update(env)


def _build_images(pool, images, platform, contexts, local_only):
    res = pool.starmap_async(
        build_image,
        [(image, version, platform, contexts, local_only) for image, version in images],
    )

    while True:
        try:
            # Have a timeout to be non-blocking for signals
            results = res.get(0.25)
            uniq_ids = []
            new_contexts = []
            new_tag_map = []
            for result in results:
                new_tag_map += result.tag_map
                new_contexts.append(result.contexts)
                uniq_ids.append(result.uniq_id)
            return new_tag_map, new_contexts, uniq_ids
            break
        except multiprocessing.context.TimeoutError:
            pass


def write_manifest_information(contexts, uniq_ids):
    manifest_data = Path(".") / "manifest_data.txt"
    manifest_data.write_text("\n".join(contexts) + "\n")

    uniq_id_data = Path(".") / "uniq_ids.txt"
    uniq_id_data.write_text("\n".join(uniq_ids) + "\n")


def read_manifest_information():
    manifest_data = Path(".") / "manifest_data.txt"
    uniq_id_data = Path(".") / "uniq_ids.txt"

    contexts = [line for line in manifest_data.read_text().splitlines() if line != ""]
    uniq_ids = [line for line in uniq_id_data.read_text().splitlines() if line != ""]
    return contexts, uniq_ids


def _build_all_images(parallel: int, platform: str, local_only: bool = False):
    images = find_images()
    validate(images)
    sorted_images = sort_images(images)

    tag_maps = []
    contexts = []
    uniq_ids = []

    if parallel == 1:
        images = [img_conf.image for img_conf in sorted_images]
        for image, version in images:
            new_tag_map, new_context, new_uniq_ids = build_image(
                image, version, platform, contexts, local_only
            )
            tag_maps += new_tag_map
            contexts.append(new_context)
            uniq_ids += new_uniq_ids
    else:
        utils.logger.info(f"Building {len(sorted_images)} images in {parallel} threads")
        utils.logger.remove()
        utils.logger.add(sys.stderr, enqueue=True, level="INFO")

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        with Pool(
            parallel, initializer=init_pool, initargs=(utils.logger, os.environ.copy())
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
                    new_tag_map, new_contexts, new_uniq_ids = _build_images(
                        pool, images, platform, contexts, local_only
                    )
                    tag_maps += new_tag_map
                    contexts += new_contexts
                    uniq_ids += new_uniq_ids
                except KeyboardInterrupt:
                    utils.logger.error("Caught KeyboardInterrupt, terminating workers")
                    pool.terminate()
                    raise

    # Write the contexts for the multi-arch image stitching
    write_manifest_information(tag_maps, uniq_ids)


@click.group(help="Manage docker images")
def cli():
    pass


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
    type=str,
    help="Name of the platform, linux/amd64, linux/arm64, windows/amd64, ...",
    show_default=True,
)
@click.option(
    "--local-only/--upload",
    default=False,
    type=bool,
    help="Keep the images local and don't upload, for security scans.",
    show_default=True,
)
def build(parallel: int, platform: Optional[str] = None, local_only: bool = False):
    if platform is None:
        machine = platformlib.machine().lower()
        os = platformlib.system().lower()
        if machine == "x86_64":
            machine = "amd64"
        if machine == "aarch64":
            machine = "arm64"
        if os == "darwin":
            # On Mac we want to typically build linux containers
            os = "linux"
        platform = f"{os}/{machine}"

    _build_all_images(parallel, platform, local_only)


@cli.command(help="Combine manifests to a multi-arch image")
def merge_manifests():
    contexts, uniq_ids = read_manifest_information()
    image_contexts = {}
    for context in contexts:
        image, sha256 = context.split(" ")
        if image not in image_contexts:
            image_contexts[image] = []
        image_contexts[image].append(sha256)

    for image in image_contexts:
        create_manifest(image, image_contexts[image])

    # TODO: Figure out how to delete these, the obvious methods don't seem to work at least with GHCR
    # for uniq_id in uniq_ids:
    #     remove_image_tag(uniq_id)


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
