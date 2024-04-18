#!/bin/bash

#
# Extract the first slide from each PDF present in the `slides/` directory as a `jpg` using
# ImageMagick. Used in README for previews.
#

for f in slides/*.pdf; do
    convert "${f}[0]" "${f/.pdf/.jpg}"
done
