#' @export
sks_last_updated <- function(
    feed_name,
    base_url = "https://geodpags.skogsstyrelsen.se/geodataport",
    datetime_format = "%Y-%m-%dT%H:%M:%SZ",
    string_format = "%Y%m%d_%H%M%S") {

  feed_url <- glue::glue("{base_url}/feeds/{feed_name}.xml")
  # download_url <- glue::glue("{base_url}/data/{file_name}.zip")

  last_updated <- xml2::read_xml(feed_url) |>
    xml2::xml_ns_strip() |>
    xml2::xml_find_all("updated") |>
    xml2::xml_text() |>
    readr::parse_datetime(datetime_format) |>
    base::format(string_format)

  last_updated
}

# Only download file if it does NOT exist in download directory
# Potential problem - if previous download was incomplete/interrupted?
#' @export
download_cmd <- function(download_url, download_file) {
  if (!file.exists(download_file)) {
    system(
      command = "cmd.exe",
      input = glue::glue("curl -o {download_file} {download_url}"),
      show.output.on.console = TRUE)
  } else {
    cat(glue::glue("Data file {download_file} already exists!"), "\n")
  }
}

#' @export
list_files_compare <- function(
    base_dir = "./downloads",
    prefix = "sksAvverkAnm",
    match_pattern = glue::glue("{prefix}\\_([0-9]{{8}})_?([0-9]{{6}})?.zip"),
    drop_na = TRUE
) {

  out <- tibble::tibble(
    original = list.files(base_dir, full.names = FALSE)
  ) |>
    dplyr::filter(
      stringr::str_detect(original, prefix)
    ) |>
    dplyr::mutate(
      created_date = stringr::str_match(original, match_pattern)[,2],
      created_time = stringr::str_match(original, match_pattern)[,3],
      created_date = readr::parse_date(created_date, "%Y%m%d"),
      created_time = readr::parse_time(created_time, "%H%M%S")
    ) |>
    dplyr::arrange(created_date, created_time) |>
    dplyr::mutate(
      revised = dplyr::lead(original)
    )

  if (drop_na) {
    out <- out |>
      tidyr::drop_na(revised)
  }

  out
}

# Should first_appearance be kept as NA when adding first records to table?
# The reasoning behind keeping NAs is that we don´t have the true
# first occurence in the database, only the first occurence in downloaded data.

#' @export
create_db_version_control <- function(
    .data,
    dsn_out,
    layer_out,
    status = "valid",
    first_appearance = NULL,
    index_field = "Beteckn",
    index_name = "idx_avverkanm_beteckn",
    append = FALSE,
    log_table_name = "log_data",
    unlink = FALSE) {

  if (unlink) {
    if (file.exists(dsn_out)) unlink(dsn_out)
  }

  sf::st_write(
    .data |>
      dplyr::mutate(
        status = status,
        first_appearance = dplyr::case_when(
          !is.null(first_appearance) ~ first_appearance,
          TRUE ~ NA_character_),
        last_appearance = NA_character_
      ),
    dsn = dsn_out,
    layer = layer_out,
    append = FALSE)

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn_out)
  con_tbl <- DBI::dbListTables(con)
  # DBI::dbSendQuery(
  #   con,
  #   "SELECT load_extension('mod_spatialite');")

  # Create index
  DBI::dbExecute(
    con,
    glue::glue("CREATE INDEX {index_name} ON {layer_out} ({index_field});")
  )

  # Create table
  # Should add date_added, date_modified, and version_number
  if (!log_table_name %in% con_tbl) {
    DBI::dbExecute(
      con,
      glue::glue(
      "CREATE TABLE {log_table_name}(
      original VARCHAR(40),
      revised VARCHAR(40),
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

#' @export
find_pending_db_updates <- function(
    start_date,
    dsn_out,
    log_table_name = "log_data",
    ...) {

  input_files <- list_files_compare(...) |>
    dplyr::filter(created_date >= start_date)

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn_out)
  log_table <- DBI::dbReadTable(con, log_table_name) |>
    tibble::as_tibble()

  DBI::dbDisconnect(con)

  input_files_pending <- input_files |>
    dplyr::anti_join(
      log_table |>
        dplyr::mutate(
          original = stringr::str_c(original, ".zip"),
          revised = stringr::str_c(revised, ".zip")),
      dplyr::join_by(original, revised))

  input_files_pending

}

# IMPORTANT: the update of attributes in the deleted data set is incorrect,
# must fix how last_appearance is updated, by changing to a correct
# WHERE-critera.
#' @export
execute_db_updates <- function(
    original,
    revised,
    query = NA,
    quiet = FALSE,
    base_dir = "downloads",
    dsn_out,
    layer_out,
    compare_attributes = "Lannr;Lan;Kommunnr;Kommun;ArendeAr;Beteckn;Avverktyp;Skogstyp;Inkomdatum;AnmaldHa;SkogsodlHa;NatforHa;ArendeStat;AvvSasong;AvvHa;Avverkning",
    match_type = 1, # 0 ("Exact Match") or 1 ("Tolerant Match (Topological Equality)")
    unchanged = qgisprocess::qgis_tmp_vector(),
    added = qgisprocess::qgis_tmp_vector(),
    deleted = qgisprocess::qgis_tmp_vector(),
    log_table_name = "log_data") {

  original_sf <- sf::st_read(
    file.path("/vsizip", base_dir, original),
    query = query,
    quiet = quiet)

  revised_sf <- sf::st_read(
    file.path("/vsizip", base_dir, revised),
    query = query,
    quiet = quiet)

  if (!is.na(query)) {
    sf::st_geometry(original_sf) <- "geometry"
    sf::st_geometry(revised_sf) <- "geometry"
  }

  result <- qgisprocess::qgis_run_algorithm(
    "native:detectvectorchanges",
    ORIGINAL = original_sf,
    REVISED = revised_sf,
    COMPARE_ATTRIBUTES = compare_attributes,
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
      last_appearance = gsub(".zip$", "", original)
    ) |>
    dplyr::select(last_appearance, Beteckn)

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
    glue::glue("INSERT INTO {log_table_name} (original, revised, n_original, n_revised, n_added, n_deleted, n_unchanged) VALUES (?, ?, ?, ?, ?, ?, ?)"),
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
  rm(list =
       c("original", "original_sf",
         "revised","revised_sf",
         "added", "deleted")
  )
  gc()
  qgisprocess::qgis_clean_result(result)

}

#' @export
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

#' @export
sks_log_table <- function(
    dsn = "sks_avverkning_anmald_sverige.gpkg",
    table_name = "log_data") {

  con <- DBI::dbConnect(RSQLite::SQLite(), dsn)

  # Add layer name to log table, extract layer name from variable original.
  # Sort log table, newest posts first
  log_table <- DBI::dbReadTable(con, table_name) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      layer = stringr::str_remove(original, "_.*")
    ) |>
    dplyr::group_by(layer) |>
    dplyr::arrange(
      desc(revised)
    )

  DBI::dbDisconnect(con)

  log_table
}

#' @export
tmp_export_from_gpkg <- function(
    layer_suffix,
    dsn = "sks_avverkning_anmald",
    layer_prefix = "sks_avverkning_anmald",
    download_dir = "./downloads",
    output_file = "sksAvverkAnm") {

  # Read from GeoPackage
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
    file.path(download_dir, glue("{output_file}.zip")),
    file.path(download_dir, glue::glue("{output_file}_{layer_suffix}.zip"))
  )
}

#' @export
pad_avverk_beteckn <- function(
    Beteckn,
    pattern = " ([0-9]{1,5})\\-",
    pad = "0",
    width = 5) {

  p2 <- str_c(" ", str_pad(str_match(Beteckn, pattern)[,2], pad = pad, width = width), "-")
  str_replace(Beteckn, pattern, p2)
}

# pad_avverk_beteckn(c("A 1-2023", "A 1017-2017", "A 51541-2015"))
