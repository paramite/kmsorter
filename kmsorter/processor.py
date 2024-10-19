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

from PIL import Image, UnidentifiedImageError

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


MAX_COLORS = 536870911
FILE_CACHE_KEY = 'kmsorter/processed-files'
LOAD_CHANNEL = 'kmsorter/loaded-files'


def image_to_colors(path):
    with Image.open(path) as img:
        colors = img.getcolors(MAX_COLORS)
    return colors


async def load_img(path, ctx):
    # TO-DO: don't print, log instead
    sum = await ctx.obj['redis'].hget(FILE_CACHE_KEY, path)
    new_sum = hashlib.md5()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            new_sum.update(chunk)
    new_sum = new_sum.hexdigest()
    if sum == new_sum:
        if ctx.obj['debug']:
            print('File %s has been already loaded.' % path)
        return

    msg = '%s|%s' % (path, ';'.join(['%s,%s' % 
                                    (','.join([str(i) for i in c[1]]), c[0]) 
                                    for c in colors]))
    if ctx.obj['debug']:
        print('Sending: %s' % msg)
    await ctx.obj['nats'].publish(LOAD_CHANNEL, msg.encode('utf-8'))
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
            await load_img(os.path.join(path, pth), ctx)
    else:
        await load_img(path, ctx)

    if ctx.obj['watch']:
        try:
            while True:
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

    return 0, 'Given path wa successfully loaded.'