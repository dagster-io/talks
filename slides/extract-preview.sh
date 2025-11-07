#!/bin/bash

#
# Extract the first slide from each PDF present in the `slides/` directory as a `jpg` using
# ImageMagick. Used in README for previews.
#

_script_dir="$(dirname "$(readlink -f "$0")")"

for f in "$_script_dir"/*.pdf; do
    convert "${f}[0]" "${f/.pdf/.jpg}"
done
