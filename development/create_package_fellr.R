# https://r-pkgs.org

# devtools::install_github("r-lib/devtools")
# devtools::install_github("r-lib/usethis")

# can be added to .Rprofile startup file
library(devtools)

# p <- "W:/projects/R/fellr"
# usethis::create_package(p, check_name = FALSE)

load_all()

# Must run document() to add export functions to NAMESPACE
document()

chk_pkg <- check()
glimpse(chk_pkg)
names(chk_pkg)

test()

usethis::use_mit_license()

use_git_config(
  user.name = "peterhellstrom",
  user.email = "peter.hellstrom@nrm.se"
)

usethis::use_git()
usethis::use_github()

usethis::create_github_token()

use_readme_rmd()
build_readme()

# Imports ----
usethis::use_package("glue", min_version = TRUE)
usethis::use_package("xml2", min_version = TRUE)
usethis::use_package("readr", min_version = TRUE)
usethis::use_package("tibble", min_version = TRUE)
usethis::use_package("dplyr", min_version = TRUE)
usethis::use_package("tidyr", min_version = TRUE)
usethis::use_package("purrr", min_version = TRUE)
usethis::use_package("stringr", min_version = TRUE)
usethis::use_package("rlang", min_version = TRUE)
usethis::use_package("sf", min_version = TRUE)
usethis::use_package("DBI", min_version = TRUE)
usethis::use_package("RSQLite", min_version = TRUE)
usethis::use_package("qgisprocess", min_version = TRUE)
usethis::use_package("rgee", min_version = TRUE)

usethis::use_tidy_description()

# Ignore ----
usethis::use_build_ignore(c("backup", "data-raw", "development", "examples"))

# Document data:
# https://r-pkgs.org/data.html

# Install ----
install()

# install_github("peterhellstrom/fellr")

## Load package ----
library(fellr)

## Data sets ----
usethis::use_data_raw()
