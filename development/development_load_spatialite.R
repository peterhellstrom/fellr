# Load spatialite module ----

# Work computer/NRM: I need to run RStudio as Admin in order to be able to load
# the spatialite extension.

# (The sqlite3 installation is only available from the A-account, check
# running the command sqlite3 from the terminal tab)
# But I can't access my shared network folder when I use the
# A-account!
# Or - s it possible to make sqlite3 available from my "ordinary" account?
# Why did I have to install OSGeo4W from my A-account?

# https://sqlite.org/cli.html

# Paths to sqlite3.exe file and mod_spatialite.dll:
# "C:/OSGeo4W/bin/sqlite3.exe"
# "C:/OSGeo4W/bin/mod_spatialite.dll"

dsn <- "E:/Maps/Naturvard/skogsstyrelsen/sks_data.gpkg"
swecoords::gpkg_contents(dsn)

con <- DBI::dbConnect(RSQLite::SQLite(), dsn)

# res <- DBI::dbSendQuery(con, "SELECT load_extension('mod_spatialite');")

res <- DBI::dbSendQuery(con, "SELECT load_extension('C:/OSGeo4W/bin/mod_spatialite.dll');")
DBI::dbClearResult(res)

DBI::dbDisconnect(con)
