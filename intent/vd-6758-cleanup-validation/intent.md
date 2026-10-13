# Intent

CI verification run for the Fabric bundle.

Bump `stg_sales_data` so `state:modified+` resolves to exactly that model, then drive the
full gate ladder against it. The design in `design.md` describes that model and only that
model, so `ci/design-drift` has an accurate contract to compare the manifest against.
