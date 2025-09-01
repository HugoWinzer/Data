-- file: sql/add_city_country.sql
-- Optional helper if you want to run manually (Cloud Build also runs a migration).
ALTER TABLE `${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}`
ADD COLUMN IF NOT EXISTS city STRING,
ADD COLUMN IF NOT EXISTS country STRING;
