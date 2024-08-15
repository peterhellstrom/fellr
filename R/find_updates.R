#' Title
#'
#' @param dsn
#' @param table_name
#'
#' @return
#' @export
#'
#' @examples
sks_log_table <- function(
    dsn,
    table_name = "log_data") {

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn)

  log_table <- DBI::dbReadTable(con, table_name) |>
    tibble::as_tibble() |>
    # Add layer name to log table, extract layer name from variable original.
    dplyr::mutate(
      layer = stringr::str_remove(.data$original, "_.*")
    ) |>
    dplyr::group_by(.data$layer) |>
    # Sort log table, newest posts first
    dplyr::arrange(
      dplyr::desc(.data$revised)
    )

  DBI::dbDisconnect(con)

  log_table
}

#' Title
#'
#' @param base_dir
#' @param file_name_pattern
#' @param date_pattern
#' @param date_formats
#' @param tz
#' @param drop_na
#'
#' @return
#' @export
#'
#' @examples
list_files_compare <- function(
    base_dir = "./downloads",
    file_name_pattern = "^sksAvverkAnm",
    date_pattern = "([0-9]{8})_?([0-9]{6})?",
    date_formats = c("ymd", "ymd_HMS"),
    # tz = base::Sys.timezone()
    tz = "Europe/Stockholm",
    drop_na = TRUE
) {

  x <- tibble::tibble(
    original = list.files(
      base_dir,
      full.names = FALSE,
      pattern = file_name_pattern
    )
  ) |>
    dplyr::mutate(
      created_date = lubridate::parse_date_time(
        stringr::str_extract(.data$original, date_pattern),
        orders = date_formats,
        tz = tz
      )
    ) |>
    dplyr::filter(!is.na(.data$created_date)) |>
    dplyr::arrange(.data$created_date) |>
    dplyr::mutate(
      revised = dplyr::lead(.data$original)
    )

  if (drop_na) {
    x <- x |>
      tidyr::drop_na("revised")
  }

  x
}

# Should first_appearance be kept as NA when adding first records to table?
# The reasoning behind keeping NAs is that we don´t have the true
# first occurence in the database, only the first occurence in downloaded data.

#' Title
#'
#' @param start_date
#' @param dsn_out
#' @param log_table_name
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
find_pending_db_updates <- function(
    start_date,
    dsn_out,
    log_table_name = "log_data",
    ...) {

  input_files <- list_files_compare(...) |>
    dplyr::filter(
      .data$created_date >= start_date
    )

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn_out)

  log_table <- DBI::dbReadTable(con, log_table_name) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      original = stringr::str_c(.data$original, ".zip"),
      revised = stringr::str_c(.data$revised, ".zip")
    )

  DBI::dbDisconnect(con)

  input_files_pending <- input_files |>
    dplyr::anti_join(
      log_table,
      dplyr::join_by("original", "revised")
    )

  input_files_pending

}
