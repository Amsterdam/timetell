from aiohttp import web

def get(_: web.Request):
    # language=rst
    """Handle the system health check.

    Any failure in the systems that the catalog depends upon should result in a
    status ``503`` (ie. raise `~aiohttp.web.HTTPServiceUnavailable`). If all
    systems are go status ``200`` is returned

    """
    text = "Timetell importer systemhealth is OK"
    return web.Response(text=text)
