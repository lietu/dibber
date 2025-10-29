import multiprocessing.context
import os
import signal
import sys
from multiprocessing import Pool
from pathlib import Path

import click

import dibber.utils as utils
from dibber.images import (
    build_and_upload_image,
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


def _build_images(pool, images, contexts):
    res = pool.starmap_async(
        build_and_upload_image,
        [(image, version, contexts) for image, version in images],
    )

    while True:
        try:
            # Have a timeout to be non-blocking for signals
            all_new_contexts = res.get(0.25)
            new_contexts = []
            for build_contexts in all_new_contexts:
                new_contexts += build_contexts
            return new_contexts
            break
        except multiprocessing.context.TimeoutError:
            pass


def write_manifest_information(contexts):
    manifest_data = Path(".") / "manifest_data.txt"
    manifest_data.write_text("\n".join(contexts) + "\n")


def read_manifest_information():
    manifest_data = Path(".") / "manifest_data.txt"
    return manifest_data.read_text().splitlines()


def _build_all_images(parallel: int):
    images = find_images()
    validate(images)
    sorted_images = sort_images(images)
    contexts = []

    if parallel == 1:
        images = [img_conf.image for img_conf in sorted_images]
        for image, version in images:
            new_contexts = build_and_upload_image(image, version, contexts)
            contexts += new_contexts
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
                    new_contexts = _build_images(pool, images, contexts)
                    contexts += new_contexts
                except KeyboardInterrupt:
                    utils.logger.error("Caught KeyboardInterrupt, terminating workers")
                    pool.terminate()
                    raise

            pool.close()

    # Write the contexts for the multi-arch image stitching
    write_manifest_information(contexts)


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
def build(parallel: int):
    _build_all_images(parallel)


@cli.command(help="Combine manifests to a multi-arch image")
def merge_manifests():
    contexts = read_manifest_information()
    image_contexts = {}
    for context in contexts:
        image, sha256 = context.split(" ")
        if image not in image_contexts:
            image_contexts[image] = []
        image_contexts[image].append(sha256)

    for image in image_contexts:
        create_manifest(image, image_contexts[image])


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
