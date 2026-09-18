#!/usr/bin/env zsh
set -e

rm -f data/self-checkout.db
rm -f data/self-checkout.db-shm
rm -f data/self-checkout.db-wal

echo "Database removed."
echo "Start the application again to create a fresh database with 2,000 items and 10,000 units per item."

# chmod +x reset-db.sh : gives the script permission to run. It does not delete anything.

# The first command removes the old test data; 
# the second starts your app and causes the seeder to recreate the 2,000 products and starting inventory.
# ./reset-db.sh
# ./mvnw spring-boot:run