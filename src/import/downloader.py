import aiohttp
import argparse
import asyncio
import logging
import urllib.parse
import pathlib
import typing as T
import os

import uvloop

_logger = logging.getLogger(__name__)


async def download(projectid: str, username: str, password: str,
                   sourcedir: str, targetdir: str, overwrite: bool):

    url = "https://{}.objectstore.eu/".format(projectid)

    target = pathlib.Path(targetdir)
    if target.exists() and not overwrite:
        raise FileExistsError

    url = urllib.parse.urljoin(url, sourcedir)
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
    parser = argparse.ArgumentParser(description='Timetell CSV downloader')
    parser.add_argument(
        'sourcedir',
        help='Source directory on objectstore: uploads or uploadIVCLDI')
    parser.add_argument(
        'targetdir',
        help='Target directory on local filesystem, for example: /data')
    parser.add_argument(
        '--overwrite', '-o', action='store_true',
        help='Overwrite if targetdir exists')
    parser.add_argument(
        '--verbose', '-v', dest='loglevel', action='store_const',
        const=logging.DEBUG, default=logging.INFO,
        help='set loglevel DEBUG (default INFO)')
    return parser


def cli():
    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()

    args = parser().parse_args()
    # init logging
    logging.basicConfig(level=args.loglevel)

    eventloop.run_until_complete(
        download(os.environ["OBJECTSTORE_PROJECT_ID"],
                 os.environ["OBJECTSTORE_USERNAME"],
                 os.environ["OBJECTSTORE_PASSWORD"],
                 args.sourcedir, args.targetdir, args.overwrite)
    )


if __name__ == '__main__':
    cli()
