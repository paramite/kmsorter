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
import asyncio
import hashlib
import os
import shutil
import tempfile
import time

from PIL import Image
from numpy import asarray, mean

from kmsorter import LOAD_CHANNEL, PROCESS_CHANNEL

MAX_COLORS = 536870911
IMAGE_CACHE = dict()


class MessageDecodeError(Exception):
    """Signals problems with decoding message to Image."""


def image_to_colors(path):
    with Image.open(path) as img:
        colors = img.getcolors(MAX_COLORS)
    return colors


def msg_to_data(msg):
    parts = msg.data.split('|'.encode('utf-8'), 3)
    id = parts[0].decode('utf-8')
    num = int(parts[1].decode('utf-8'))
    count = int(parts[2].decode('utf-8'))
    data = parts[3]

    img = IMAGE_CACHE.setdefault(id, dict(count=count, 
                                          data=[None for i in range(count)]))
    img['data'][num] = data

    if None in img['data']:
        return id, None

    img_data = b''
    for i in img['data']:
        img_data += i
    path = os.path.join(ctx.obj['tmpdir'], id)
    with open(path, 'wb') as img:
        img.write(data)
    with Image.open(path) as img:
        npdata = asarray(img)
    return id, npdata


async def process_images(ctx):
    ctx.obj['tmpdir'] = tempfile.mkdtemp()

    async def message_handler(msg):
        id, npdata = msg_to_data(msg)
        if npdata is None:
            return
        mean_color = mean(npdata, axis=(0, 1))
        processed = '%d,%d,%d|' % (mean_color)
        processed = processed.encode('utf-8')
        processed += msg
        await ctx.obj['nats'].publish(PROCESS_CHANNEL, msg)



    sub = await ctx.obj['nats'].subscribe(LOAD_CHANNEL, cb=message_handler)
    await ctx.obj['nats'].flush()    

    try:
        print('Waiting for images on channel %s' % LOAD_CHANNEL)
        while True:
            await asyncio.sleep(0.1)
    finally:
        await sub.unsubscribe()
        await ctx.obj['nats'].drain()        
        shutil.rmtree(ctx.obj['tmpdir'])
