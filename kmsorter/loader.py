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
import os
import time

from PIL import Image

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


MAX_COLORS = 536870911
FILE_CHUNK_SIZE = 524288
FILE_CACHE_KEY = 'kmsorter/processed-files'
LOAD_CHANNEL = 'kmsorter/loaded-files'


def convert_to_rgb(path):
    image = Image.open(path)
    if image.mode != 'RGB':
        image = image.convert('RGB')
    image.save(path)
    image.close()


def image_chunk_to_message(path, i, chunk):
    id = '%s|%d|' % (path, i)
    msg = bytearray(id.encode('utf-8'))
    msg.extend(chunk)
    return msg


async def load_img(path, ctx):
    convert_to_rgb(path)

    sum = await ctx.obj['redis'].hget(FILE_CACHE_KEY, path)
    new_sum = hashlib.md5()
    msgs = []
    with open(path, 'rb') as f:
        i = 0
        while chunk := f.read(FILE_CHUNK_SIZE):
            msgs.append(image_chunk_to_message(path, i, chunk))
            new_sum.update(chunk)

    new_sum = new_sum.hexdigest()
    if sum == new_sum:
        if ctx.obj['debug']:
            print('File %s has been already loaded.' % path)
        return

    for i, m in enumerate(msgs):
        if ctx.obj['debug']:
            print('Sending part #%d of %s' % (i, path))
        await ctx.obj['nats'].publish(LOAD_CHANNEL, m)

    await ctx.obj['redis'].hset(FILE_CACHE_KEY, path, new_sum)


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