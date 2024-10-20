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

import hashlib
import math
import os
import socket
import tempfile
import time

from PIL import Image

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from kmsorter import LOAD_CHANNEL


FILE_CHUNK_SIZE = 524288
FILE_CACHE_KEY = 'kmsorter/processed-files'


class DuplicateError(Exception):
    """Reports duplicated file load"""


def convert_to_rgb(path, ctx):
    image = Image.open(path)
    if image.mode != 'RGB':
        image = image.convert('RGB')
        out_path = os.path.join(ctx.obj['tmpdir'], os.path.basename())
        image.save(out_path)
        image.close()
        return out_path
    image.close()
    return path


def image_chunk_to_message(id, i, count, chunk):
    id = '%s|%d|%d|' % (id, i, count)
    msg = bytearray(id.encode('utf-8'))
    msg.extend(chunk)
    return msg


async def deduplicate(host, name, sum, ctx):
    # id = prefix:hostname
    # id => [hostname:basename, hostname:basename, ...]
    # cache[hostname:basename] => sum
    id = '%s:%s' % (FILE_CACHE_KEY, name)

    img_id = '%s:%s' % (host, name)
    
    isthere = await ctx.obj['redis'].exists(id)
    if isthere:
        count = await ctx.obj['redis'].llen(id)
        on_same_host = False
        for i in range(count):
            hk = await ctx.obj['redis'].rpop(id)
            hk = hk.decode('utf-8')
            host, name = hk.split(':')
            hsum = await ctx.obj['redis'].hget(FILE_CACHE_KEY, hk)
            hsum = hsum.decode('utf-8')
            await ctx.obj['redis'].lpush(id, hk)
            hkparts = hk.split(':')
            if sum == hsum:
                # report duplicate
                raise DuplicateError('Same image was processed '
                                     'by agent on host %s' % host)
            elif hkparts[0] == host:
                # same host, same name, different sum -> different file
                on_same_host = True
        else:
            if on_same_host:
                # same host, same name, do name appendix
                # since there is not actual duplicate
                parts = name.split('.')
                if len(parts) == 1:
                    nm, ext = name, ''
                else:
                    nm, ext = '.'.join(parts[:-1]), parts[-1]
                i = 0
                isthere = True
                while isthere:
                    hk = '%s:%s' % (host, '.'.join(['%s%d' % (nm, i), ext]))
                    isthere = await ctx.obj['redis'].exists(hk)
                    i += 1
                img_id = hk

    # there is not such ocurrence, so create one
    await ctx.obj['redis'].lpush(id, img_id)
    await ctx.obj['redis'].hset(FILE_CACHE_KEY, img_id, sum)
    return img_id


async def load_img(path, ctx):
    path = convert_to_rgb(path, ctx)

    sum = hashlib.md5()
    chunks = []
    with open(path, 'rb') as f:
        i = 0
        while chunk := f.read(FILE_CHUNK_SIZE):
            chunks.append(chunk)
            sum.update(chunk)
    sum = sum.hexdigest()

    try:
        img_id = await deduplicate(socket.gethostname(),
                                   os.path.basename(path),
                                   sum, ctx)
    except DuplicateError as ex:
        if ctx.obj['debug']:
            print('File %s has been already loaded: %s' % (path, ex))
        return        

    count = math.ceil(os.stat(path).st_size / FILE_CHUNK_SIZE)
    for i, c in enumerate(chunks):
        if ctx.obj['debug']:
            print('Sending part #%d of %s' % (i, path))
        msg = image_chunk_to_message(img_id, i, count, c)
        await ctx.obj['nats'].publish(LOAD_CHANNEL, msg)


class NewFileHandler(FileSystemEventHandler):
    def __init__(self, ctx, path):
        super(self).__init__()
        self.ctx = ctx
        self.path = path

    def on_created(self, event):
        if self.ctx.obj['debug']:
            click.echo('Loading file %s' % event.src_path)
        load_img(event.src_path, self.ctx)  

    def on_moved(self, event):
        if not event.dest_path.startswith(self.path):
            return
        if self.ctx.obj['debug']:
            click.echo('Loading file %s' % event.dest_path)
        load_img(event.dest_path, self.ctx)
 

async def load_images(ctx, path):
    ctx.obj['tmpdir'] = tempfile.mkdtemp()
    if ctx.obj['watch']:
        if not os.path.isdir():
            return 1, 'What is the point of watching single file?'

        observer = Observer()
        observer.schedule(NewFileHandler(ctx, path), path)
        observer.start()

        if ctx.obj['debug']:
            click.echo('Watching path %s for new files.' % path)

    if os.path.isdir(path):
        for pth in os.listdir(path):
            try:
                await load_img(os.path.join(path, pth), ctx)
            except Exception as ex:
                click.echo('Failed to load %s. Reason: %s' % (path, ex))
    else:
        try:
            await load_img(path, ctx)
        except Exception as ex:
                return 1, 'Failed to load %s. Reason: %s' % (path, ex)

    if ctx.obj['watch']:
        try:
            while True:
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

    return 0, 'Given path was successfully loaded.'