#!/bin/sh
# Run this from the root of your repo. It creates ./assets and downloads
# every image referenced in the README into it with descriptive names.
# Works with sh, bash, or zsh.
set -e
mkdir -p assets

# One "filename|url" pair per line
IMAGES='
header-icon.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/60f8f158-3bdc-4b3d-94ae-27a12441e2a3
gcs-icon.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/6b5ffddf-0f74-41cf-8303-22f675cabdda
composer-icon.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/4c57cf42-15d3-4ba3-bad6-65b7fb9c5094
dataflow-icon.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/69ceceb2-73a4-4b8b-9eb1-1207e7b5c5e3
bigquery-icon.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/48de05df-2b34-4d82-b2c5-e81809e9322c
looker-icon.png|https://seeklogo.com/images/G/google-looker-logo-B27BD25E4E-seeklogo.com.png
gcp-diagram.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/f3a7ff86-92b3-46db-a156-e5ebbefc3bb9
csv-sample.png|https://github.com/janaom/gcp-data-engineering-project-food-orders-etl/assets/83917694/285dcfd6-f212-418b-b5bc-e56beb35fa52
beam-mascot.png|https://beam.apache.org/images/mascot/beam_mascot_500x500.png
beam-shell-run-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/5f26e09a-3b98-4848-9413-097a49a84bd6
beam-shell-run-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/8d18241f-4ede-431e-b123-744ed9470f0c
composer-icon-large.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/c1d5bfc6-94c3-44e4-b38e-4eaf5656a840
composer1-create-env.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/f5f7d40b-67fe-4206-9501-92b042c950f7
composer1-env-created.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/a9bb70e4-0cef-4290-ba6a-c81e587046f9
composer1-upload-beam.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/4fe512ed-489a-4955-b289-89d72be61dcf
composer1-gsutil-uri-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/593511d3-fde2-4704-8c3e-030037802419
composer1-gsutil-uri-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/8601ea31-2c88-42d6-9441-bf5576b0e19e
composer1-upload-dag.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/91ef65c4-37de-42b4-85be-85187a4db78c
composer1-dag-ui-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/52cfa6e7-e577-412c-963c-2861dc2eb4cf
composer1-dag-ui-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/e671c5ef-ed4c-470d-b96a-636119e9b847
composer1-gcs-sensor-log.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/378cd73f-d29c-43d4-9d3f-3e5cf3c754d9
composer1-list-files-log.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/f1689521-8ce8-4444-b2f1-83677a6d1ac9
composer1-processed-folder.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/9451caf3-61db-4612-915f-0e3938bef965
composer1-beamtask-started.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/03784888-c23d-43ae-9b15-f3383dd984fe
composer1-dataflow-completed-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/922be238-59f7-413e-9415-f6cda9b772f5
composer1-dataflow-completed-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/cef67aea-f2ce-4d7a-8047-5a46cc237758
composer1-bigquery-results.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/eebb4d73-6332-4a90-8798-e81cc5b2f628
composer1-new-file-upload.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/6a16b07e-7bc3-4522-9fb6-ff94347ffa63
composer1-append-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/a4d449d0-35ec-42dc-b6b6-c08e91934b95
composer1-append-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/12d734e3-d0ae-44ff-9bd6-f1e8b27859e1
composer1-xcoms.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/1e26c216-0a18-475d-98eb-367883d7469a
composer2-operators.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/9d918734-cd98-49eb-a973-586d6178d341
composer2-env-size.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/64581910-e4e2-4196-8812-4478a5af0739
composer2-setup-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/a7198bbd-db3c-49b3-b8d8-dd7b6e50d690
composer2-setup-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/22293adc-98b2-42ba-932e-e565867b7862
composer2-setup-3.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/c4c5018d-7eb3-448e-875c-7c975c1200a9
composer2-setup-4.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/f28d8fa7-84f1-4e0a-8184-575aeb9e2ece
composer2-dag-ui.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/2867c0f1-922e-48c8-a657-7563a9afbdcb
composer2-graph-1.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow-TEST/assets/83917694/1e6cc0bb-28a2-497c-be2d-a73a27a99d9b
composer2-graph-2.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow-TEST/assets/83917694/42c60726-f7da-4e0c-8d88-161a69e4fc69
composer2-manual-trigger.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/4b889475-d7ab-4c62-81eb-97f153b2bb91
composer2-dataflow-running.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/4fe108f0-ddce-458e-83fa-159a4860f186
looker-report.png|https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/ebca078f-f231-4f86-9938-c0cf026c0c97
'

echo "$IMAGES" | while IFS='|' read -r name url; do
  [ -z "$name" ] && continue
  echo "Downloading $name ..."
  curl -sL "$url" -o "assets/$name"
done

echo "Done. Check assets/ for downloaded files, then git add/commit/push."