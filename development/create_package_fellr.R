# https://r-pkgs.org

# devtools::install_github("r-lib/devtools")
# devtools::install_github("r-lib/usethis")

library(devtools) # can be added to .Rprofile startup file

# p <- "W:/projects/R/fellr"
# usethis::create_package(p, check_name = FALSE)

load_all()

# Must run document() to add export functions to NAMESPACE
document()
install()

chk_sweapi <- check()
glimpse(chk_chk_sweapi)
names(chk_chk_sweapi)

test()

usethis::use_mit_license()

use_git_config(user.name = "peterhellstrom", user.email = "peter.hellstrom@nrm.se")
usethis::use_git()
usethis::use_github()

usethis::create_github_token()

use_readme_rmd()
build_readme()

# Ignore ----
usethis::use_build_ignore(c("backup", "data-raw", "development", "examples"))

# Document data:
# https://r-pkgs.org/data.html

install_github("peterhellstrom/sweapi")

## Load package ----
library(sweapi)

## Data sets ----
usethis::use_data_raw()
