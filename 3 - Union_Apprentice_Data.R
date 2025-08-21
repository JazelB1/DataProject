# Load the packages
library(aws.s3)
library(readr) # For read_csv
library(dplyr)
library(purrr)
library(stringr) 

# --- S3 Configuration ---
aws_access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
region_aws <-Sys.getenv("AWS_REGION")

bucket_name <- "new-apprentice-raw-data" 
s3_folder_path <- "raw_excel_data/" 



# --- Define Columns to Select and Standardize ---
columns_to_keep <- c(
  "Apprentice Number" = "APPRENTICE_NUMBER",
  "Program Number" = "Program Number",
  "Occupation" = "Occupation",
  "Industry" = "Industry",
  "NAICS Code" = "NAICS_CD",
  "Exit Date" = "Exit Date",
  "Start Date" = "Start Date",
  "State" = "HUD_STATE (program location)",
  "Region" = "Region",
  "Sex" = "Sex",
  "Race" = "Race",
  "Age" = "Age Cohort",
  "Apprentice Status Code" = "Apprentice Status Code",
  "New Apprentice" = "NEW_APPR",
  "Fiscal Year" = "Fiscal Year",
  "Total apprentice number" = "Total_apprentice_num"
)

# --- Define desired FINAL column types ---
desired_final_col_types <- list(
  "Apprentice Number" = as.character,
  "Program Number" = as.character,
  "Occupation" = as.character,
  "Industry" = as.character,
  "NAICS Code" = as.character,
  "Exit Date" = function(x) as.Date(x, format = "%m/%d/%Y"),
  "Start Date" = function(x) as.Date(x, format = "%m %d %Y %H:%M:%S"),
  "State" = as.character,
  "Region" = as.character,
  "Sex" = as.character,
  "Race" = as.character,
  "Age" = as.character,
  "Apprentice Status Code" = as.character,
  "New Apprentice" = as.integer,    # Use as.integer for whole numbers
  "Fiscal Year" = function(x) as.Date(x, format = "%m/%d/%Y"),
  "Total apprentice number" = as.integer
)


# --- Listing and Filtering Files from S3 ---
message("Listing CSV files in S3 bucket: ", bucket_name, " at path: ", s3_folder_path)
s3_objects_list <- tryCatch({
  get_bucket(bucket = bucket_name, prefix = s3_folder_path)
}, error = function(e) {
  stop(paste("Failed to list S3 objects. Check bucket name, path, and credentials. Error:", e$message))
})

target_files_s3 <- s3_objects_list %>%
  map_chr(~ .x$Key) %>%
  str_subset(pattern = "(?i)\\.csv$")

message("\n--- Debugging: Files identified by script ---")
if (length(target_files_s3) == 0) {
  message("  No CSV files were identified by the script after filtering. Check folder path and extensions.")
  stop("No CSV files found in the specified S3 folder.")
} else {
  message(paste("  Script identified", length(target_files_s3), "CSV file(s)."))
  print(target_files_s3)
}
message("--------------------------------------------\n")

# --- Initialize all_data list ---
all_data <- list()
successful_files_count <- 0
errors_log <- list() # Initialize errors_log

# --- Downloading, Processing, and Unioning Files ---
for (file_key in target_files_s3) {
  message(paste("\nProcessing file:", file_key))
  
  temp_file <- tempfile(fileext = ".csv")
  
  processing_success <- FALSE
  current_file_error <- NULL
  
  tryCatch({
    message(paste("  Attempting to download:", file_key))
    aws.s3::save_object(object = file_key, bucket = bucket_name, file = temp_file)
    message(paste("  Successfully downloaded:", file_key))
    
    message(paste("  Attempting to read CSV:", temp_file))
    df_raw <- readr::read_csv(temp_file) # Removed show_col_types = FALSE to see more warnings
    
    parsing_problems <- problems(df_raw)
    if (nrow(parsing_problems) > 0) {
      warning(paste("  Parsing issues found in", basename(file_key), ". Top 5 issues:\n",
                    paste(capture.output(head(parsing_problems, 5)), collapse = "\n")))
    }
    message(paste("  Successfully read CSV:", basename(file_key)))
    
    message(paste("  Raw columns in", basename(file_key), ":", paste(names(df_raw), collapse = ", ")))
    
    # --- Column Selection and Renaming ---
    # Create a list to hold the processed data frame
    temp_processed_df <- data.frame(row.names = seq_len(nrow(df_raw))) # Start with empty df with correct num rows
    
    for (new_name in names(columns_to_keep)) {
      original_name <- unname(columns_to_keep[new_name])
      
      if (original_name %in% names(df_raw)) {
        # If the original column exists, get its data
        column_data <- df_raw[[original_name]]
      } else {
        # If the original column does not exist, fill with NA
        warning(paste("  Desired column '", new_name, "' (original '", original_name, "') missing from", basename(file_key)))
        column_data <- NA
      }
      
      # Apply type conversion immediately
      if (!is.null(desired_final_col_types[[new_name]])) {
        conversion_fn <- desired_final_col_types[[new_name]]
        column_data <- suppressWarnings(conversion_fn(column_data))
      }
      
      # Add the (converted) column to the temporary processed_df
      temp_processed_df[[new_name]] <- column_data
    }
    
    # Assign temp_processed_df to processed_df
    processed_df <- temp_processed_df
    
    # Add source_file column
    processed_df$source_file <- basename(file_key)
    
    all_data[[file_key]] <- processed_df
    successful_files_count <- successful_files_count + 1
    processing_success <- TRUE
    
  }, error = function(e) {
    current_file_error <<- e
    error_msg <- if (!is.null(e$message)) e$message else
      if (!is.null(e$call)) paste("Error in call:", deparse(e$call)[1]) else
        "Unknown error type (check errors_log object for full details)."
    message(paste("  FAILED to process file:", file_key, "- Details:", error_msg))
    errors_log[[file_key]] <- e # Store the full error object
  }, finally = {
    if (file.exists(temp_file)) {
      unlink(temp_file)
    }
  })
}


# --- Unioning Processed DataFrames ---
if (successful_files_count > 0) {
  message("\nAttempting to union ", successful_files_count, " successfully processed CSV file(s)...")
  final_union_df <- purrr::list_rbind(all_data)
  
  message("\nSuccessfully unioned all CSV files!")
  print(head(final_union_df))
  print(paste("Total rows in unioned data:", nrow(final_union_df)))
  print(paste("Total columns in unioned data:", ncol(final_union_df)))
  
  
  
  # --- Save DataFrame to S3 as CSV ---
  message("\n--- Saving final_union_df to S3 ---")
  
  # 1. Define the S3 output path and filename
  output_s3_folder <- "processed_apprentice_data/" # Or "your_output_folder/"
  output_filename <- paste0("union_apprentice_data_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
  s3_output_key <- paste0(output_s3_folder, output_filename) # Full path in S3
  
  # 2. Create a temporary local file to write the CSV to
  local_temp_output_file <- tempfile(fileext = ".csv")
  
  tryCatch({
    # 3. Write the data frame to the local temporary CSV file
    write_csv(final_union_df, local_temp_output_file, na = "", append = FALSE) 
    
    # 4. Upload the local temporary CSV file to S3
    message(paste0("  Uploading '", basename(local_temp_output_file), "' to s3://", bucket_name, "/", s3_output_key))
    aws.s3::put_object(
      file = local_temp_output_file,
      object = s3_output_key,
      bucket = bucket_name,
      multipart = TRUE 
    )
    message(paste0("  Successfully saved data to s3://", bucket_name, "/", s3_output_key))
    
  }, error = function(e) {
    warning(paste0("  Failed to save data to S3. Error: ", e$message))
  }, finally = {
    # 5. Clean up the local temporary file, regardless of success or failure
    if (file.exists(local_temp_output_file)) {
      unlink(local_temp_output_file)
      message("  Cleaned up local temporary file.")
    }
  })
  
} else {
  message("No data frames were successfully processed. 'all_data' is empty. Nothing to union.")
  final_union_df <- data.frame()
} 
  
