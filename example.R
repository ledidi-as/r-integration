# install.packages(c("dplyr", "tidyr", "httr", "jsonlite", "lubridate"))

source("ledidi.R")

# Add project ID as number
project_id <- 0
token <- "INSERT REFRESH TOKEN"

df <- get_data(project_id, token, include_metadata = TRUE, use_category_labels = TRUE)

df_series <- get_series_data("SERIES NAME", project_id, token, include_metadata = FALSE)

df_merged <- get_merged_series_data("SERIES NAME", project_id, token, include_metadata = FALSE)
