#' Title
#'
#' @param input
#' @param output
#' @param driver
#' @param assign_srs
#' @param new_layer_name
#' @param new_layer_type
#'
#' @returns
#' @export
#'
#' @examples
sks_import <- function(
    input,
    output,
    driver = "GPKG",
    assign_srs = "EPSG:3006",
    new_layer_name,
    new_layer_type = "PROMOTE_TO_MULTI"
) {
  
  gdal_options <-
    c(
      "-f", driver,
      "-a_srs", assign_srs,
      "-nln", new_layer_name,
      "-nlt", new_layer_type,
      "-mapFieldType", "Integer64=Real",
      "-overwrite"
    )
  
  sf::gdal_utils(
    util = "vectortranslate",
    source = input,
    destination = output,
    options = gdal_options
  )
}
