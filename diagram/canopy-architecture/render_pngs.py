#!/usr/bin/env python3
"""Render the SVG atlas at twice its viewBox resolution with librsvg."""
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parent
renderer = shutil.which('rsvg-convert')
if renderer is None:
    raise SystemExit('rsvg-convert is required for this local PNG export helper.')
for svg in sorted(root.glob('*.svg')):
    dims = list(map(float, ET.parse(svg).getroot().attrib['viewBox'].split()))
    png = svg.with_name(svg.stem+'@2x.png')
    subprocess.run([renderer, '-w', str(int(dims[2]*2)), '-h', str(int(dims[3]*2)),
                    '-o', str(png), str(svg)], check=True)
    print(png.name)
