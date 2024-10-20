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
    
    if 
    img = IMAGE_CACHE.setdefault(hostname)



async def process_images(ctx):
   