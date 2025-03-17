# IMPORTANT: the update of attributes in the deleted data set is incorrect,
# must fix how last_appearance is updated, by changing to a correct
# WHERE-criteria.

#' Title
#'
#' @param original
#' @param revised
#' @param query
#' @param quiet
#' @param base_dir
#' @param dsn_out
#' @param layer_out
#' @param compare_attributes
#' @param match_type
#' @param unchanged
#' @param added
#' @param deleted
#' @param log_table_name
#'
#' @returns
#' @export
#'
#' @examples
execute_db_updates <- function(
    original,
    revised,
    query = NA,
    base_dir = "downloads",
    dsn_out,
    layer_out,
    compare_attributes =
      c(
        "Lannr", "Lan", "Kommunnr", "Kommun", "ArendeAr", "Beteckn",
        "Avverktyp", "Skogstyp", "Inkomdatum",
        "AnmaldHa", "SkogsodlHa", "NatforHa",
        "ArendeStatus", "AvvSasong", "AvvHa", "AvverkningsanmalanKlass",
        "Andamal"
      ),
    match_type = 1, # 0 ("Exact Match") or 1 ("Tolerant Match (Topological Equality)")
    unchanged = qgisprocess::qgis_tmp_vector(),
    added = qgisprocess::qgis_tmp_vector(),
    deleted = qgisprocess::qgis_tmp_vector(),
    log_table_name = "log_data",
    load_spatialite = "SELECT load_extension('mod_spatialite');"
) {

  original_sf <- sf::read_sf(
    file.path("/vsizip", base_dir, original),
    query = query
  ) |>
    mutate(
      across(where(is.character), str_trim)
    )

  revised_sf <- sf::read_sf(
    file.path("/vsizip", base_dir, revised),
    query = query
  ) |>
    mutate(
      across(where(is.character), str_trim)
    )

  # Describe this step!
  if (!is.na(query)) {
    sf::st_geometry(original_sf) <- "geometry"
    sf::st_geometry(revised_sf) <- "geometry"
  }

  result <- qgisprocess::qgis_run_algorithm(
    "native:detectvectorchanges",
    ORIGINAL = original_sf,
    REVISED = revised_sf,
    COMPARE_ATTRIBUTES =
      stringr::str_c(compare_attributes, collapse = ";"),
    MATCH_TYPE = match_type,
    UNCHANGED = unchanged,
    ADDED = added,
    DELETED = deleted
  )

  added <- sf::st_as_sf(result$ADDED)
  deleted <- sf::st_as_sf(result$DELETED)
  # It is not necessary to read unchanged records to memory
  # unchanged <- st_as_sf(result$UNCHANGED)

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn_out)

  # Load spatialite extension:
  # If not, we get an error message.
  # "Error: no such function: ST_IsEmpty"
  res <- DBI::dbSendQuery(con, load_spatialite)

  update_deleted <- deleted |>
    sf::st_drop_geometry() |>
    dplyr::mutate(
      last_appearance = gsub(".zip$", "", original)
    ) |>
    dplyr::select(
      "last_appearance", "Beteckn"
    )

  if (nrow(update_deleted) > 0) {
    DBI::dbExecute(
      con,
      glue::glue(
        "UPDATE {layer_out} SET status = 'deleted', last_appearance = ? WHERE (Beteckn = ? AND last_appearance IS NULL)"
      ),
      params = update_deleted |>
        as.list() |>
        rlang::set_names(NULL)
    )
  }

  if (nrow(added) > 0) {
    sf::st_write(
      added |>
        dplyr::mutate(
          status = "valid",
          first_appearance = gsub(".zip$", "", revised),
          last_appearance = NA_character_
        ),
      dsn = dsn_out,
      layer = layer_out,
      append = TRUE)
  }

  # Add data to log-table
  DBI::dbExecute(
    con,
    glue::glue(
      "INSERT INTO {log_table_name} (original, revised, n_original, n_revised, n_added, n_deleted, n_unchanged) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ),
    params = list(
      gsub(".zip$", "", original),
      gsub(".zip$", "", revised),
      nrow(original_sf),
      nrow(revised_sf),
      result$ADDED_COUNT,
      result$DELETED_COUNT,
      result$UNCHANGED_COUNT
    )
  )

  # Results should be cleared higher up in the code, revise!
  DBI::dbClearResult(res)
  DBI::dbDisconnect(con)

  # Free used memory
  rm(
    list = c(
      "original", "original_sf",
      "revised","revised_sf",
      "added", "deleted"
    )
  )

  on.exit(gc())
  on.exit(qgisprocess::qgis_clean_result(result))

}


#' Title
#'
#' @param original
#' @param revised
#' @param query
#' @param base_dir
#' @param dsn_out
#' @param layer_out
#' @param compare_attributes
#' @param match_type
#' @param unchanged
#' @param added
#' @param deleted
#' @param log_table_name
#'
#' @returns
#' @export
#'
#' @examples
execute_db_updates_memory <- function(
    original,
    revised,
    original_str,
    revised_str,
    dsn_out,
    layer_out,
    compare_attributes =
      c(
        "Lannr", "Lan", "Kommunnr", "Kommun", "ArendeAr", "Beteckn",
        "Avverktyp", "Skogstyp", "Inkomdatum",
        "AnmaldHa", "SkogsodlHa", "NatforHa",
        "ArendeStatus", "AvvSasong", "AvvHa", "AvverkningsanmalanKlass"
      ),
    match_type = 1, # 0 ("Exact Match") or 1 ("Tolerant Match (Topological Equality)")
    unchanged = qgisprocess::qgis_tmp_vector(),
    added = qgisprocess::qgis_tmp_vector(),
    deleted = qgisprocess::qgis_tmp_vector(),
    log_table_name = "log_data") {

  result <- qgisprocess::qgis_run_algorithm(
    "native:detectvectorchanges",
    ORIGINAL = original,
    REVISED = revised,
    COMPARE_ATTRIBUTES = stringr::str_c(compare_attributes, collapse = ";"),
    MATCH_TYPE = match_type,
    UNCHANGED = unchanged,
    ADDED = added,
    DELETED = deleted
  )

  added <- sf::st_as_sf(result$ADDED)
  deleted <- sf::st_as_sf(result$DELETED)
  # It is not necessary to read unchanged records to memory
  # unchanged <- st_as_sf(result$UNCHANGED)

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn_out)

  # Load spatialite extension:
  # If not, we get an error message.
  # "Error: no such function: ST_IsEmpty"
  # But why and where is this extension necessary here?
  res <- DBI::dbSendQuery(con, "SELECT load_extension('mod_spatialite');")

  update_deleted <- deleted |>
    sf::st_drop_geometry() |>
    dplyr::mutate(
      last_appearance = original_str
    ) |>
    dplyr::select("last_appearance", "Beteckn")

  if (nrow(update_deleted) > 0) {
    DBI::dbExecute(
      con,
      glue::glue("UPDATE {layer_out} SET status = 'deleted', last_appearance = ? WHERE (Beteckn = ? AND last_appearance IS NULL)"),
      params = update_deleted |>
        as.list() |>
        rlang::set_names(NULL)
    )
  }

  if (nrow(added) > 0) {
    sf::st_write(
      added |>
        dplyr::mutate(
          status = "valid",
          first_appearance = revised_str,
          last_appearance = NA_character_
        ),
      dsn = dsn_out,
      layer = layer_out,
      append = TRUE)
  }

  # Add data to log-table
  DBI::dbExecute(
    con,
    glue::glue(
      "INSERT INTO {log_table_name} (original, revised, n_original, n_revised, n_added, n_deleted, n_unchanged) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ),
    params = list(
      original_str,
      revised_str,
      nrow(original),
      nrow(revised),
      result$ADDED_COUNT,
      result$DELETED_COUNT,
      result$UNCHANGED_COUNT
    )
  )

  # Results should be cleared higher up in the code, revise!
  DBI::dbClearResult(res)
  DBI::dbDisconnect(con)

  # Free used memory
  rm(
    list =
      c("original", "original_sf",
        "revised","revised_sf",
        "added", "deleted")
  )
  gc()
  qgisprocess::qgis_clean_result(result)

}

#' Title
#'
#' @param .data
#' @param dsn_out
#' @param layer_out
#' @param ...
#' @param n
#'
#' @returns
#' @export
#'
#' @examples
execute_db_updates_n <- function(
    .data,
    dsn_out,
    layer_out,
    ...,
    n = base::seq_len(nrow(.data))) {

  if (nrow(.data) > 0) {
    purrr::walk(
      .x = n,
      \(x) {
        execute_db_updates(
          .data$original[x],
          .data$revised[x],
          dsn_out = dsn_out,
          layer_out = layer_out,
          ...
        )
      }
    )
  }
}

