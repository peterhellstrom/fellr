#' Title
#'
#' @param .data
#' @param dsn
#' @param layer
#' @param status
#' @param first_appearance
#' @param append
#'
#' @return
#' @export
#'
#' @examples
create_initial_data <- function(
    .data, dsn, layer, status = "valid", first_appearance = NULL, append = FALSE
) {
  sf::st_write(
    .data |>
      dplyr::mutate(
        status = status,
        first_appearance = dplyr::case_when(
          !is.null(first_appearance) ~ first_appearance,
          TRUE ~ NA_character_),
        last_appearance = NA_character_
      ),
    dsn = dsn,
    layer = layer,
    append = append
  )
}

#' Title
#'
#' @param dsn
#' @param layer
#' @param index_name
#' @param index_field
#'
#' @return
#' @export
#'
#' @examples
create_index <- function(dsn, layer, index_name, index_field) {

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn)
  con_tbl <- DBI::dbListTables(con)
  # DBI::dbSendQuery(
  #   con,
  #   "SELECT load_extension('mod_spatialite');")

  # Create index
  DBI::dbExecute(
    con,
    glue::glue("CREATE INDEX {index_name} ON {layer} ({index_field});")
  )

  on.exit(
    DBI::dbDisconnect(con)
  )

}

#' Title
#'
#' @param dsn
#' @param log_table_name
#'
#' @return
#' @export
#'
#' @examples
create_log_table <- function(dsn, log_table_name = "log_data") {

  # Create log table
  con <- DBI::dbConnect(RSQLite::SQLite(), dsn)
  con_tbl <- DBI::dbListTables(con)

  # Should add date_added, date_modified, and version_number?
  # VARCHAR datatype not supported?
  if (!log_table_name %in% con_tbl) {
    DBI::dbExecute(
      con,
      glue::glue(
      "CREATE TABLE {log_table_name}(
      original TEXT,
      revised TEXT,
      n_original INTEGER,
      n_revised INTEGER,
      n_added INTEGER,
      n_deleted INTEGER,
      n_unchanged INTEGER)"
      )
    )
  }

  on.exit(
    DBI::dbDisconnect(con)
  )
}

#' Title
#'
#' @param .data
#' @param dsn_out
#' @param layer_out
#' @param status
#' @param first_appearance
#' @param index_field
#' @param index_name
#' @param append
#' @param log_table_name
#' @param unlink
#'
#' @return
#' @export
#'
#' @examples
create_gpkg_version_control <- function(
    .data,
    dsn,
    layer,
    status = "valid",
    first_appearance = NULL,
    index_field = "Beteckn",
    index_name = "idx_avverkanm_beteckn",
    append = FALSE,
    log_table_name = "log_data",
    unlink = FALSE
) {

  if (unlink) {
    if (file.exists(dsn)) unlink(dsn)
  }

  create_initial_data(
    .data = .data, dsn = dsn, layer = layer,
    status = status, first_appearance = first_appearance, append = append
  )
  create_index(dsn = dsn, index_name = index_name, index_field = index_field)
  create_log_table(dsn = dsn, log_table_name = log_table_name)

}
