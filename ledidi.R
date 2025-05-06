library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)


URL <- "https://sw1eigoed4.execute-api.eu-central-1.amazonaws.com/prod/getstats"


get_data <- function(project_id, token, include_metadata = FALSE, use_category_labels = TRUE) {
  access_token <- get_access_token(token)
  
  more_data <- TRUE
  start_at <- 0
  df_list <- list()
  i <- 1
  
  while (more_data) {
    payload <- create_payload(project_id, access_token, start_at)
    response <- request_data(URL, payload)
    
    if (response$httpStatusCode != 200) {
      stop(response$message)
    }
    
    df_list[[i]] <- as_tibble(response$dataRows)
    more_data <- response$moreData
    start_at <- start_at + nrow(response$dataRows)
    i <- i + 1
  }
  
  metadata <- get_metadata(project_id, access_token)
  var_metadata <- get_var_metadata(metadata, response)
  
  result <- transform_data(df_list, var_metadata, use_category_labels) 
  
  if (!include_metadata) {
    result <- select(result, -starts_with("ledidi_"))
  }
  
  return (result)
}


get_series_data <- function(series_name, project_id, token, include_metadata = FALSE, use_category_labels = TRUE) {
  access_token <- get_access_token(token)
  
  metadata <- get_metadata(project_id, access_token)
  series_id <- get_series_id(metadata, series_name)
  
  more_data <- TRUE
  start_at <- 0
  df_list <- list()
  i <- 1
  
  while (more_data) {
    payload <- create_payload(project_id, access_token, start_at, series_id)
    response <- request_data(URL, payload)
    
    if (response$httpStatusCode != 200) {
      stop(response$message)
    }
    
    df_list[[i]] <- as_tibble(response$dataRows)
    more_data <- response$moreData
    start_at <- start_at + nrow(response$dataRows)
    i <- i + 1
  }
  
  var_metadata <- get_var_metadata(metadata, response)
  
  result <- transform_data(df_list, var_metadata, use_category_labels = TRUE, is_series = TRUE)
  
  if (!include_metadata) {
    result <- select(result, -starts_with("ledidi_"))
  }
  
  return (result)
}


get_merged_series_data <- function(series_name, project_id, token, include_metadata = FALSE, use_category_labels = TRUE) {
  df_main <- get_data(project_id, token, include_metadata = TRUE, use_category_labels)
  df_series <- get_series_data(series_name, project_id, token, include_metadata = TRUE, use_category_labels)
  
  result <- 
    df_main |> 
    left_join(
      df_series, 
      by = join_by(ledidi_entry_id == ledidi_parent_id),
      suffix = c("_main", "_series")
    ) |> 
    select(
      everything(),
      -ends_with("_main"),
      ledidi_entry_id, 
      -ledidi_parent_id,
      -ledidi_entry_id_series
    )
    
  if (!include_metadata) {
    result <- select(result, -ends_with("_series"))
  } else {
    result <- 
      result |> 
      rename_with(
        .fn = ~gsub("_series", "", .x, fixed = TRUE),
        .cols = ends_with("_series")
      ) 
  }
  
  return (result)
    
}



# Utilities ---------------------------------------------------------------

get_access_token <- function(refresh_token) {
  url <- "https://cognito-idp.eu-central-1.amazonaws.com/"
  headers <- c(
    `accept` = '*/*',
    `accept-language` = 'no,en-US;q=0.9,en;q=0.8',
    `cache-control` = 'no-store',
    `content-type` = 'application/x-amz-json-1.1',
    `origin` = 'https://app.ledidi.no',
    `priority` = 'u=1, i',
    `referer` = 'https://app.ledidi.no/',
    `sec-ch-ua-mobile` = '?0',
    `sec-fetch-dest` = 'empty',
    `sec-fetch-mode` = 'cors',
    `sec-fetch-site` = 'cross-site',
    `x-amz-target` = 'AWSCognitoIdentityProviderService.InitiateAuth',
    `x-amz-user-agent` = 'aws-amplify/6.13.5 framework/0'
  )
  body_json_string <- sprintf(
    '{"ClientId":"6qbb0nnajnse4em6d1d3pc3gpo","AuthFlow":"REFRESH_TOKEN_AUTH","AuthParameters":{"REFRESH_TOKEN":"%s"}}',
    refresh_token
  )
  response <- POST(
    url = url,
    add_headers(.headers = headers),
    body = body_json_string
  )
  
  fromJSON(content(response, "text", encoding = "UTF-8"))$AuthenticationResult$AccessToken
}


create_payload <- function(project_id, token, start_at, series_id = NA) {
  if (!is.na(series_id)) {
    result <- paste0(
      '{
          "method": "getRepeatingSetRowsV2",
          "projectId": ', project_id, ',
          "set": {"setName": "', series_id,'"},
          "startAt": ', start_at, ',
          "maxPageSize": 20000,
          "filters": [],
          "accessTokenStr": "', token, '"
      }'
    )
  } else {
    result <- paste0(
      '{
          "method": "getDatasetRowsV2",
          "projectId": ', project_id, ',
          "startAt": ', start_at, ',
          "maxPageSize": 20000,
          "filters": [],
          "accessTokenStr": "', token, '"
      }'
    )
  }
  return (result)
}


request_data <- function(url, payload) {
  POST(url, body = payload)  |> 
    content(as = "text", encoding = "UTF-8") |>
    fromJSON()
}


add_category_labels <- function(var_metadata, metadata) {
  mapping <- tibble(old_name = character(), category_map = list())
  
  for (i in 1:length(metadata$variables)) {
    var <- metadata$variables[[i]]
    if (var$variableType == "category" & length(var$allowedCategories) > 0) {
      mapping <- 
        mapping |> 
        add_row(
          old_name = var$variableName, 
          category_map = list(split(var$allowedCategories$label, var$allowedCategories$value))
        )
    }
  }
  
  var_metadata |> 
    left_join(mapping, by = "old_name")
  
}


rename_var_names <- function(df, var_metadata, is_series = FALSE) {
  new_names <- pull(var_metadata, new_name)
  old_names <- pull(var_metadata, old_name)
  
  df <- 
    df |> 
    rename_with(~new_names[which(old_names == .x)], .cols = all_of(old_names))
  
  df[, new_names]
}


cast_data_types <- function(df, var_metadata) {
  date_vars <- get_vars_with_type(var_metadata, "date")
  datetime_vars <- get_vars_with_type(var_metadata, "datetime")
  category_cars <- get_vars_with_type(var_metadata, "category")
  
  df |> 
    mutate(
      across(all_of(date_vars), ~as_date(., format = "%Y-%m-%d")),
      across(all_of(datetime_vars), ~as.POSIXct(sub("T(.*?)((\\+|\\-|Z).*)$", " \\1", .), format = "%Y-%m-%d %H:%M:%S")),
      across(all_of(category_cars), as.character),
      ledidi_created_date = as_datetime(ledidi_created_date, tz = "UTC"),
      ledidi_last_modified_date = as_datetime(ledidi_last_modified_date, tz = "UTC")
    )
}


transform_category_values <- function(df, var_metadata, use_category_values) {
  var_metadata_filtered <- 
    var_metadata |> 
    tidyr::drop_na(category_map)
  
  if (nrow(var_metadata_filtered) > 0 & use_category_values) {
    for (i in 1:nrow(var_metadata_filtered)) {
      var_name <- 
        var_metadata_filtered |> 
        filter(row_number() == i) |> 
        pull(new_name)
      category_map <- (
        var_metadata_filtered |>
          filter(row_number() == i) |>
          pull(category_map)
      )[[1]]
      
      df <- 
        df |> 
        mutate(!!var_name := recode(get(var_name), !!!category_map))  
      
    }
  }
  
  return (df)
}


transform_data <- function(df_list, var_metadata, use_category_labels, is_series = FALSE) {
  
  # Remove statuses
  df_list <- lapply(df_list, function(df) {
    cols_to_keep <- !sapply(df, is.data.frame) & names(df) != "none_generated_by_ledidi"
    df[, cols_to_keep, drop = FALSE]
  })
  
  df <- 
    bind_rows(df_list) |> 
    rename_var_names(var_metadata, is_series) |> 
    cast_data_types(var_metadata) |> 
    transform_category_values(var_metadata, use_category_labels)
  
}


get_vars_with_type <- function(var_metadata, var_type) {
  var_metadata |>
    filter(type == var_type) |>
    pull(new_name)
}


get_metadata <- function(project_id, token) {
  payload <- paste0(
    '{
          "method": "getVariablesData",
          "projectId": ', project_id, ',
          "accessTokenStr": "', token, '"
      }'
  )
  response <- request_data(URL, payload)
  
  if (response$httpStatusCode != 200) {
    stop(response$message)
  }
  
  return (response)
  
}


get_var_metadata <- function(metadata, response) {
  
  result <- 
    response$variables |> 
    select(
      old_name = variableName,
      new_name = variableLabel,
      type = variableType
    ) |> 
    filter(type != "status") |> 
    update_var_names(metadata) |> 
    add_category_labels(metadata)
  
  result[which(result$old_name == "original_id"), "new_name"] <- "ledidi_entry_id"
  result[which(result$old_name == "ownedbyuser"), "new_name"] <- "ledidi_entry_owner"
  result[which(result$old_name == "creationdate"), "new_name"] <- "ledidi_created_date"
  result[which(result$old_name == "enteredbyuser"), "new_name"] <- "ledidi_last_modified_by"
  result[which(result$old_name == "lastmodifieddate"), "new_name"] <- "ledidi_last_modified_date"
  result[which(result$old_name == "userProjectOrgId"), "new_name"] <- "ledidi_organisation_id"
  result[which(result$old_name == "parentsetrevisionid"), "new_name"] <- "ledidi_parent_id"
  result[which(result$old_name == "datasetentryid"), "new_name"] <- "ledidi_revision_id"
  
  return(result)
}


update_var_names <- function(var_metadata, metadata) {
  
  series_var_names <- metadata$variables
  series_metadata <- metadata$sets
  if (length(series_metadata) == 0) {
    return (var_metadata)
  }
  for (i in 1:length(series_metadata)) {
    agg_rules <- series_metadata[[i]]$aggregationRules
    if (length(agg_rules) == 0) {
      next
    }
    for (j in 1:nrow(agg_rules)) {
      agg_var_id <- agg_rules[j, "name"]
      agg_rule <- agg_rules[j, "rule"]$type
      var_id <- agg_rules[j, "aggregator"]$variableName
      var_name <- series_var_names[[var_id]]$variableLabel
      agg_var_name <- paste0(var_name, " (", toupper(agg_rule), ")")
      
      var_metadata <-
        var_metadata |> 
        mutate(new_name = replace(new_name, new_name == agg_var_id, agg_var_name))
    }
  }
  
  return (var_metadata)
}


get_series_id <- function(metadata, series_name) {
  series_metadata <- metadata$sets
  result <- NA
  for (i in 1:length(series_metadata)) {
    if (series_name == series_metadata[[i]]$setLabel) {
      result <- series_metadata[[i]]$setName
    }
  }
  if (is.na(result)) stop("The series is not found. Try correcting the name.")
  
  return (result)
}