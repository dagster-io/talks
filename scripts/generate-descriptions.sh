#!/bin/bash

#
# Generate descriptions from presentations using Claude CLI.
# Extracts text from PDFs and uses Claude to generate 1-2 sentence summaries.
# Can process a single file or all PDFs in the slides/ directory.
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

# Function to generate description using Claude CLI
generate_with_claude() {
    local content="$1"
    local title="$2"

    claude --print <<EOF
You are a technical writer creating a 1-2 sentence description for a presentation.

Title: $title

Requirements:
- Write exactly 1-2 sentences
- Use action-oriented language: "Learn how...", "Explore...", "See how...", "Discover..."
- Focus on what attendees will learn or gain
- Match this style: "Learn how to use Dagster and DuckDB to build a Github deduplication pipeline"
- Return ONLY the description text, no preamble, no explanation, no quotes

Presentation content:
$content

Description:
EOF
}

# Function to process a single PDF file
process_pdf() {
    local pdf_path="$1"
    local filename=$(basename "$pdf_path")

    # Extract text (first 5 pages, 10K chars max)
    local text=$(pdftotext -f 1 -l 5 "$pdf_path" - 2>/dev/null | head -c 10000)

    if [ -z "$text" ]; then
        echo "  Warning: No text extracted from $filename"
        return 1
    fi

    # Generate description with Claude CLI
    local description=$(generate_with_claude "$text" "$filename")

    # Output result
    echo ""
    echo "[$filename]"
    echo "$description"
    echo ""

    return 0
}

# Function to process a markdown source file
process_markdown() {
    local md_path="$1"
    local filename=$(basename "$md_path")

    # Read markdown content, skip frontmatter (first ~15 lines), take 10K chars
    local text=$(tail -n +15 "$md_path" 2>/dev/null | head -c 10000)

    if [ -z "$text" ]; then
        echo "  Warning: No content extracted from $filename"
        return 1
    fi

    # Generate description with Claude CLI
    local description=$(generate_with_claude "$text" "$filename")

    # Output result
    echo ""
    echo "[$filename]"
    echo "$description"
    echo ""

    return 0
}

# Main processing logic
if [ -n "$1" ]; then
    # Single file mode: process the specified file
    if [ ! -f "$1" ]; then
        echo "Error: File not found: $1"
        exit 1
    fi

    case "$1" in
        *.pdf)
            process_pdf "$1"
            ;;
        *.md)
            process_markdown "$1"
            ;;
        *)
            echo "Error: Unsupported file type. Only .pdf and .md files are supported."
            exit 1
            ;;
    esac
else
    # Batch mode: process all PDFs in slides directory
    pdf_count=$(find "$_slides_dir" -maxdepth 1 -name "*.pdf" -type f | wc -l)
    if [ "$pdf_count" -eq 0 ]; then
        echo "No PDF files found in $_slides_dir"
        exit 0
    fi

    echo "Found $pdf_count PDF file(s) in $_slides_dir..."

    # Counter
    generated=0

    # Process all PDFs
    for f in "$_slides_dir"/*.pdf; do
        # Skip if glob didn't match any files
        [ -e "$f" ] || continue

        if process_pdf "$f"; then
            ((generated++))
        fi
    done

    echo ""
    echo "✓ Complete: $generated descriptions generated"
fi
