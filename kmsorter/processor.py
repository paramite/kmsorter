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

import asyncio
import hashlib
import os
import shutil
import tempfile
import time

from PIL import Image
from numpy import asarray, mean

from kmsorter import LOAD_CHANNEL

MAX_COLORS = 536870911
IMAGE_CACHE = dict()


class MessageDecodeError(Exception):
    """Signals problems with decoding message to Image."""


def image_to_colors(path):
    with Image.open(path) as img:
        colors = img.getcolors(MAX_COLORS)
    return colors


def msg_to_image(msg):
    pcs = msg.split('|'.encode('utf-8'))
    if len(pcs) != 5:
        raise MessageDecodeError('Failed to split message: %s' % msg)
    for i in range(4):
        pcs[i] = pcs[i].decode('utf-8')

    img = IMAGE_CACHE.setdefault(hostname)



async def process_images(ctx):
    ctx.obj['tmpdir'] = tempfile.mkdtemp()

    async def message_handler(msg):
        parts = msg.data.split('|'.encode('utf-8'), 3)
        id = parts[0].decode('utf-8')
        num = int(parts[1].decode('utf-8'))
        count = int(parts[2].decode('utf-8'))
        data = parts[3]

        path = os.path.join(ctx.obj['tmpdir'], id)
        with open(path, 'wb') as img:
            img.write(data)
        with Image.open(path) as img:
            npdata = asarray(img)

        mean_color = mean(npdata, axis=(0, 1))
   

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
