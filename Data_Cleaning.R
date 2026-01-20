library(httr)
library(readxl)
library(dplyr)
library(stringr)
library(janitor)

# Raw GitHub URLs

latest_url   <- "https://raw.githubusercontent.com/SGM-Econ/Regional-Labour-Market/main/data/nomis_latest.xlsx"
historic_url <- "https://raw.githubusercontent.com/SGM-Econ/Regional-Labour-Market/main/data/historicdata.xlsx"

# Download both files to temp paths

tmp_latest <- tempfile(fileext = ".xlsx")
tmp_hist   <- tempfile(fileext = ".xlsx")

GET(latest_url,   write_disk(tmp_latest, overwrite = TRUE))
GET(historic_url, write_disk(tmp_hist,   overwrite = TRUE))

# Final desired DF names

region_names <- c(
  "UK", "NEAST", "NWEST", "YORKS", "EMIDS", "WMIDS",
  "EAST", "LON", "SEAST", "SWEST", "WAL", "SCOT", "NIRE"
)

# Correct order for HISTORIC workbook (by sheet position, after Contents is ignored)
historic_order <- c(
  "UK", "EAST", "EMIDS", "LON", "NEAST", "NWEST", "NIRE",
  "SCOT", "SEAST", "SWEST", "WAL", "WMIDS", "YORKS"
)

# Read from row 11, then truncate after first all-NA row
read_clean_sheet <- function(path, sheet) {
  
  df <- read_excel(
    path,
    sheet = sheet,
    skip = 10,        # start at row 11
    col_names = TRUE
  )
  
  # Drop everything after the first all-NA row
  first_all_na <- which(apply(df, 1, function(x) all(is.na(x))))[1]
  
  if (!is.na(first_all_na) && first_all_na > 1) {
    df <- df[1:(first_all_na - 1), , drop = FALSE]
  } else if (!is.na(first_all_na) && first_all_na == 1) {
    df <- df[0, , drop = FALSE]
  }
  
  df %>% clean_names()
}

# Get sheet names (use by POSITION, not by name)

sheets_latest_all <- excel_sheets(tmp_latest)
sheets_hist_all   <- excel_sheets(tmp_hist)

# Guardrails

stopifnot(length(sheets_latest_all) >= 13)
stopifnot(length(sheets_hist_all) >= 14)

# Latest: first 13 sheets

latest_sheets_13 <- sheets_latest_all[2:14]

# Historic: ignore first sheet (Contents), then take next 13 sheets

hist_sheets_13 <- sheets_hist_all[2:14]

# Map region -> sheet name in each workbook using the known orders

latest_sheet_for_region <- setNames(latest_sheets_13, region_names)
hist_sheet_for_region   <- setNames(hist_sheets_13, historic_order)

# Import, append latest onto historic, assign into Global Env using region_names

for (reg in region_names) {
  
  df_hist   <- read_clean_sheet(tmp_hist,   hist_sheet_for_region[[reg]])
  df_latest <- read_clean_sheet(tmp_latest, latest_sheet_for_region[[reg]])
  
  df_out <- bind_rows(df_hist, df_latest)
  
  assign(reg, df_out, envir = .GlobalEnv)
}

# Rename columns x2-x7 in each data frame

for (nm in region_names) {
  df <- get(nm, envir = .GlobalEnv)
  
  if (all(paste0("x", 2:7) %in% names(df))) {
    names(df)[match(paste0("x", 2:7), names(df))] <-
      c("POP16", "TOTEMP", "TOTUNEMP", "POP1664", "TOTEMP1664", "TOTEINACT1664")
  }
  
  assign(nm, df, envir = .GlobalEnv)
}

# Drop spare df called df (only if it exists)

if (exists("df", envir = .GlobalEnv)) rm(df, envir = .GlobalEnv)
if (exists("d", envir = .GlobalEnv)) rm(d, envir = .GlobalEnv)


# Add URATE / ERATE / EIRATE to each regional data frame
for (nm in region_names) {
  d <- get(nm, envir = .GlobalEnv)
  
  d <- d %>%
    mutate(
      across(c(TOTUNEMP, TOTEMP, TOTEMP1664, POP1664, TOTEINACT1664), as.numeric),
      URATE  = 100 * TOTUNEMP / (TOTUNEMP + TOTEMP),
      ERATE  = 100 * TOTEMP1664 / POP1664,
      EIRATE = 100 * TOTEINACT1664 / POP1664
    )
  
  assign(nm, d, envir = .GlobalEnv)
}

# Remove specified objects (if they exist)
rm(list = intersect(c("Contents", "d", "df_combined", "df_hist", "df_latest", "df_out"), ls(envir = .GlobalEnv)),
   envir = .GlobalEnv)


                              # Save combined clean data object to /data
clean_data <- setNames(
  lapply(region_names, function(nm) get(nm, envir = .GlobalEnv)),
  region_names
)

if (!dir.exists("data")) dir.create("data", recursive = TRUE)

saveRDS(clean_data, file.path("data", "clean data.rds"))


