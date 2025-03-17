#' Title
#'
#' @param feed_name
#' @param base_url
#' @param datetime_format
#' @param string_format
#'
#' @returns
#' @export
#'
#' @examples
sks_last_updated <- function(
    feed_name,
    base_url = "https://geodpags.skogsstyrelsen.se/geodataport",
    datetime_format = "%Y-%m-%dT%H:%M:%SZ",
    string_format = "%Y%m%d_%H%M%S") {
  
  feed_url <- glue::glue("{base_url}/feeds/{feed_name}.xml")
  
  last_updated <- xml2::read_xml(feed_url) |>
    xml2::xml_ns_strip() |>
    xml2::xml_find_all("updated") |>
    xml2::xml_text() |>
    readr::parse_datetime(datetime_format) |>
    base::format(string_format)
  
  last_updated
}
