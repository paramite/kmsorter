#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  Copyright 2024 Martin Magr <martin.magr@gmail.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#

import asyncclick as click
import nats
import redis.asyncio as redis
import sys

from functools import wraps

from kmsorter.loader import load_images
#from .processor import process_img
#from .sorter import sort_img


async def connect_nats(uri):
    try:
        conn = await nats.connect(uri)
        msg = ''
    except OSError as ex:
        conn = None
        msg = str(ex)
    return conn, msg


async def connect_redis(uri):
    prot, addr = uri.split('://')
    host, port = addr.split(':')
    try:
        conn = redis.Redis(host=host, port=port, db=0, protocol=3)
        await conn.ping()
        msg = ''
    except Exception as ex:
        conn = None
        msg = str(ex)
    return conn, msg


async def disconnect(ctx):
    await ctx.obj['redis'].aclose()
    await ctx.obj['nats'].drain()


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('-n', '--nats-uri',
              default='nats://localhost:4222',
              help='URI to NATS service is required '
                   'for passing data between components')
@click.option('-r', '--redis-uri',
              default='redis://localhost:6379',
              help='URI to Redis service is required '
                   'for component synchronization')
@click.pass_context
async def main(ctx, debug, nats_uri, redis_uri):
    """Image sorter app"""
    ctx.ensure_object(dict)
    ctx.obj['debug'] = debug
    nats_conn, nats_msg = await connect_nats(nats_uri)
    redis_conn, redis_msg = await connect_redis(redis_uri)
   
    if nats_conn is None:
        click.echo('Failed to connect to NATS service: %s' % nats_msg)
        sys.exit(1)
    elif redis_conn is None:
        click.echo('Failed to connect to Redis service: %s' % redis_msg)
        sys.exit(1)
    else:
        if debug:
            click.echo('Successfuly connected to backend services')
        ctx.obj['nats'] = nats_conn
        ctx.obj['redis'] = redis_conn


@main.command()
@click.option('--watch/--no-watch',
              is_flag=True, default=False,
              help='If set to watch the command runs in service mode '
                   'in given directory. Otherwise only single given '
                   'image is loaded.')
@click.argument('path', required=True)
@click.pass_context
async def load(ctx, path, watch):
    """Runs image loader in given directory path."""
    if ctx.obj['debug']:
        click.echo('Starting image loader')
    ctx.obj['watch'] = watch
    rc, msg = await load_images(ctx, path)
    click.echo(msg)
    await disconnect(ctx)
    sys.exit(rc)


@main.command()
@click.pass_context
async def process(ctx):
    """Runs image processor of loaded images."""
    if ctx.obj['debug']:
        click.echo('Starting image processor')


@main.command()
@click.option('-c', '--color', multiple=True,
              help='Color in hexadecimal format for example "#0E96B4"')
@click.pass_context
async def sort(ctx, color=None):
    """Runs image sorter of processed images. Images are sorted
    based on the given colors."""
    if ctx.obj['debug']:
        click.echo('Starting image sorter')


if __name__ == '__main__':
    main(_anyio_backend="asyncio")
