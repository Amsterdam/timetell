import aiohttp
import argparse
import asyncio
import logging
import urllib.parse
import pathlib
import typing as T
import os
import errno

import uvloop

_logger = logging.getLogger(__name__)
loglevel = None


def create_dir_if_not_exists(directory):
    """
    Create directory if it does not yet exists.
    Args:
        Specify the name of directory, for example: `dir/anotherdir`
    Returns:
        Creates the directory if it does not exists, or return the error message.
    """
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


async def download(projectid: str, username: str, password: str,  # noqa: E999
                   container: str, targetdir: str, overwrite: bool):
    """
    Logs in to stack.cloudvps.com with projectid, username and password
    And download all files from the given container to the targetdir.
    """
    url = "https://{}.objectstore.eu/".format(projectid)

    create_dir_if_not_exists(targetdir)

    target = pathlib.Path(targetdir)
    if not overwrite:
        raise FileExistsError

    url = urllib.parse.urljoin(url, container)
    _logger.debug("Downloading file listing from %s", url)
    headers = {
        'Accept': 'application/json'
    }

    headers['Authorization'] = aiohttp.BasicAuth(username, password).encode()

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception("Unexpected response code while fetching file listing: {}".format(response.status))
            filelist = await response.json()
        await asyncio.gather(*[
            _download_file(
                urllib.parse.urljoin(url + '/', f['name']),
                target / f['name'], session, headers['Authorization']
            )
            for f in filelist
        ])


async def _download_file(url: str, target: pathlib.Path, session: aiohttp.ClientSession, authz: str):
    """
    Download a file from the given url using a session with a given authz header.
    Writes the file to the target path.
    """
    # remove target if exists
    if target.exists():
        target.unlink()
    async with session.get(url, headers={'Authorization': authz}) as response:
        if response.status != 200:
            raise Exception("Unexpected response code while fetching file listing: {}".format(response.status))
        _logger.debug("Downloading %s", target)
        with target.open(mode='wb') as f:
            chunk = await response.content.read(1024)
            while chunk:
                f.write(chunk)
                chunk = await response.content.read(1024)
        _logger.debug("Done downloading %s", target)


def parser():
    """
    Command line arguments
    """
    parser = argparse.ArgumentParser(description='Timetell CSV downloader')
    parser.add_argument(
        '-c', '--container', default='uploads',
        help='Source directory on objectstore: uploads or uploadIVCLDI')
    parser.add_argument(
        '-t', '--targetdir', default='data',
        help='Target directory on local filesystem, for example: /data')
    parser.add_argument(
        '-o', '--overwrite', action='store_true', default=True,
        help='Overwrite if targetdir exists')
    parser.add_argument(
        '-v', '--verbose',
        action='store_const', dest=loglevel,
        const=logging.DEBUG, default=logging.INFO,
        help='set loglevel to logging.DEBUG or logging.INFO')
    return parser


def cli():
    """
    Download all files from a container from the objectstore
    using the stored environment variables:
    - OBJECTSTORE_PROJECT_ID
    - OBJECTSTORE_USERNAME
    - OBJECTSTORE_PASSWORD

    Using the passed arguments:
    - container
    - targetdir
    - overwrite (default: True)
    - verbose (default: logging.INFO)
    """

    args = parser().parse_args()
    # init logging
    logging.basicConfig(level=loglevel)

    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()

    eventloop.run_until_complete(
        download(os.environ["OBJECTSTORE_PROJECT_ID"],
                 os.environ["OBJECTSTORE_USERNAME"],
                 os.environ["OBJECTSTORE_PASSWORD"],
                 args.container, args.targetdir, args.overwrite)
    )


if __name__ == '__main__':
    cli()
