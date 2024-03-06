from dagster import Definitions, load_assets_from_modules

from . import assets

order_assets = load_assets_from_modules(modules=[assets])

defs = Definitions(
    assets=order_assets
)