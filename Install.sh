# Update package liste
sudo apt-get update

# Install Postgres database and packages
sudo apt-get install postgresql-client-9.4 # client libraries and client binaries
sudo apt-get install postgresql-9.4 # core database server
sudo apt-get install postgresql-contrib-9.4 # additional supplied modules
sudo apt-get install pgadmin3 # pgAdmin III graphical administration utility
sudo apt-get install python-psycopg2 # PostgreSQL database adapter for the Python programming language

# PostgreSQL Apt Repository
## Create the file /etc/apt/sources.list.d/pgdg.list, and add a line for the repository 
deb http://apt.postgresql.org/pub/repos/apt/ wheezy-pgdg main

## Import the repository signing key, and update the package lists 
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
