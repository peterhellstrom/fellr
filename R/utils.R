utils::globalVariables(".data")

#' @export
pad_avverk_beteckn <- function(
    Beteckn,
    pattern = " ([0-9]{1,5})\\-",
    pad = "0",
    width = 5) {

  p2 <- stringr::str_c(
    " ", stringr::str_pad(
      stringr::str_match(Beteckn, pattern)[,2],
      pad = pad, width = width
    ), "-"
  )

  stringr::str_replace(Beteckn, pattern, p2)
}

# pad_avverk_beteckn(c("A 1-2023", "A 1017-2017", "A 51541-2015"))

