import aiohttp
import argparse
import asyncio
import logging
import urllib.parse
import pathlib
import typing as T

import uvloop

_CLOUDVPS_URL = "https://{projectid}.objectstore.eu/"

_logger = logging.getLogger(__name__)


async def download(cloudvps_project: str, user: T.Optional[str],
                   sourcedir: str, targetdir: str, overwrite: bool):
    # first create the targetpath
    target = pathlib.Path(targetdir)
    if target.exists() and not overwrite:
        raise FileExistsError
    elif not target.exists():
        target.mkdir(parents=True)
    # do work
    url = _CLOUDVPS_URL.format(projectid=cloudvps_project)
    url = urllib.parse.urljoin(url, sourcedir)
    _logger.debug("Downloading file listing from %s", url)
    headers = {
        'Accept': 'application/json'
    }
    if user is not None:
        assert ':' in user, 'User must be <user:password>; no colon found ' \
                            'in {}'.format(user)
        user, password = user.split(':', maxsplit=1)
        headers['Authorization'] = aiohttp.BasicAuth(user, password).encode()
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


async def _download_file(url:str, target: pathlib.Path, session: aiohttp.ClientSession, authz: str):
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


def cli():
    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()

    # parse arguments
    parser = argparse.ArgumentParser(description='Timetell CSV downloader')
    parser.add_argument(
        'cloudvpsproject', metavar='PROJECTID', help='CloudVPS project ID')
    parser.add_argument(
        'sourcedir', metavar='PATH', help='Source directory on objectstore')
    parser.add_argument(
        'targetdir', metavar='PATH', help='Target directory on local filesystem')
    parser.add_argument(
        '--user', '-u', action='store', metavar='<user:password>',
        help='Specify the user name and password to use for objectstore '
             'authentication.')
    parser.add_argument(
        '--overwrite', '-o', action='store_true',
        help='Overwrite if targetdir exists')
    parser.add_argument(
        '--verbose', '-v', dest='loglevel', action='store_const',
        const=logging.DEBUG, default=logging.INFO,
        help='set loglevel DEBUG (default INFO)')
    args = parser.parse_args()

    # init logging
    logging.basicConfig(level=args.loglevel)

    # download files
    eventloop.run_until_complete(
        download(args.cloudvpsproject, args.user, args.sourcedir, args.targetdir, args.overwrite)
    )
