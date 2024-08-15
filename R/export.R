#' Title
#'
#' @param layer_suffix
#' @param dsn
#' @param layer_prefix
#' @param download_dir
#' @param output_file
#'
#' @return
#' @export
#'
#' @examples
tmp_export_from_gpkg <- function(
    layer_suffix,
    dsn = "sks_avverkning_anmald",
    layer_prefix = "sks_avverkning_anmald",
    download_dir = "./downloads",
    output_file = "sksAvverkAnm") {
  
  # Read from GeoPackage
  # This step - is it necessary?
  f_tmp <- sf::st_read(
    stringr::str_c(dsn, ".gpkg"),
    layer = glue::glue("{layer_prefix}_{layer_suffix}")
  )
  
  # Export to zip-file
  zipfile <- rgee::ee_utils_shp_to_zip(
    f_tmp,
    file.path(download_dir, glue::glue("{output_file}.shp"))
  )
  
  # Rename zip-file
  file.rename(
    file.path(download_dir, glue::glue("{output_file}.zip")),
    file.path(download_dir, glue::glue("{output_file}_{layer_suffix}.zip"))
  )
}
