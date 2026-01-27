# CONTRIBUTING.md

## Adding a talk

Copy the PDF file to the `slides/` directory.

```
cp ~/Downloads/deep-dive-dagster-modal-demo.pdf slides/.
```

Run the script to generate an image preview of the first slide.

```
make generate-thumbnails
```

And then create an entry at the top of README.md with the talk details.

```
## 2024-09-24 - Deep Dive - Dagster Modal Demo

Author(s): Charles Frye, Colton Padden

Orchestrating ML Workloads with Dagster & Modal

[[Source Code]](https://github.com/dagster-io/dagster-modal-demo)
[[Video]](https://www.youtube.com/watch?v=z_4KBYsyjks)

<div align="center">
  <a href="https://github.com/dagster-io/talks/blob/main/slides/deep-dive-dagster-modal-demo.pdf">
     <img height="250" src="slides/deep-dive-dagster-modal-demo.jpg" />
  </a>
</div>
```
