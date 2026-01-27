#!/bin/bash

#
# Generate JPG thumbnails from the first slide of each PDF in the `slides/` directory
# using ImageMagick. Skips PDFs that already have corresponding thumbnails.
# Used in README for previews.
#

# Get the repository root (parent of scripts directory)
_script_dir="$(dirname "$(readlink -f "$0")")"
_repo_root="$(dirname "$_script_dir")"
_slides_dir="$_repo_root/slides"

# Verify slides directory exists
if [ ! -d "$_slides_dir" ]; then
    echo "Error: slides directory not found at $_slides_dir"
    exit 1
fi

# Count PDFs to process
pdf_count=$(find "$_slides_dir" -maxdepth 1 -name "*.pdf" -type f | wc -l)
if [ "$pdf_count" -eq 0 ]; then
    echo "No PDF files found in $_slides_dir"
    exit 0
fi

echo "Found $pdf_count PDF file(s) in $_slides_dir..."

# Counters
generated=0
skipped=0

# Generate thumbnails for PDFs without existing JPGs
for f in "$_slides_dir"/*.pdf; do
    # Skip if glob didn't match any files
    [ -e "$f" ] || continue

    output="${f/.pdf/.jpg}"

    if [ -f "$output" ]; then
        echo "  Skipping: $(basename "$f") (thumbnail exists)"
        ((skipped++))
    else
        echo "  Generating: $(basename "$f") -> $(basename "$output")"
        convert "${f}[0]" "$output"
        ((generated++))
    fi
done

echo ""
echo "✓ Complete: $generated generated, $skipped skipped"
