library(readxl)
library(aws.s3)
   
   # --- Configuration ---
   # Replace with your actual AWS credentials and bucket details
aws_access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
region_aws <-Sys.getenv("AWS_REGION")
bucket_name <- "new-apprentice-raw-data" # Replace with your bucket name
s3_prefix <- "raw_excel_data/"           # Optional: Add a prefix/folder in your bucket
   
     # Path to the directory containing your Excel files
excel_directory <- "/Users/jazelbarquero/Documents/R Studio Project/New Apprentices - Raw Data" # Replace with the actual path
     
       # --- Function to upload a single Excel file to S3 ---
upload_excel_to_s3 <- function(file_path, bucket, key) {
  tryCatch({
    # Read the Excel file (you can choose a specific sheet if needed)
    excel_data <- read_excel(file_path)
                     
     # Create a temporary CSV file in memory
    temp_csv <- tempfile(fileext = ".csv")
    write.csv(excel_data, temp_csv, row.names = FALSE)
                       
     # Upload the temporary CSV file to S3
    put_object(
      file = temp_csv,
      object = key,
      bucket = bucket,
      access_key = aws_access_key,
      secret_key = aws_secret_key,
      region = region_aws,
      multipart = TRUE
    )
                       
  # Clean up the temporary CSV file
    unlink(temp_csv)
                       
    cat(paste("Successfully uploaded:", basename(file_path), "to s3://", bucket, "/", key, "\n"))
                       
  }, error = function(e) {
    cat(paste("Error uploading:", basename(file_path), "-", e$message, "\n"))
  })
}
       
# --- List all Excel files in the specified directory ---
excel_files <- list.files(path = excel_directory, pattern = "\\.(xlsx|xls)$", full.names = TRUE)
         
# --- Upload each Excel file to S3 ---
if (length(excel_files) > 0) {
  cat("Found", length(excel_files), "Excel files to upload...\n")
  for (file in excel_files) {
    # Create the S3 object key (filename with optional prefix)
    file_name_without_extension <- tools::file_path_sans_ext(basename(file))
    s3_key <- paste0(s3_prefix, file_name_without_extension, ".csv")
                         
    # Upload the file
    upload_excel_to_s3(file, bucket_name, s3_key)
  }
  cat("Finished processing Excel files.\n")
} else {
  cat("No Excel files found in the specified directory.\n")
}

