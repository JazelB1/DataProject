# --- 1. Install and Load Required Packages ---
# If you don't have these packages installed, uncomment and run the lines below:
# install.packages("aws.s3", dependencies = TRUE)
# install.packages("dplyr")
# install.packages("lubridate") # For easy date manipulation

library(aws.s3)
library(dplyr)
library(lubridate) # Used for extracting year from date

# --- 2. AWS S3 Configuration (IMPORTANT: Do NOT hardcode credentials in production) ---
# Option A: If credentials are set as environment variables or in ~/.aws/credentials,
#           you might not need to set them explicitly here.
# Option B: Set credentials explicitly (replace with your actual credentials)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "YOUR_ACCESS_KEY_ID")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "YOUR_SECRET_ACCESS_KEY")
# Sys.setenv("AWS_DEFAULT_REGION" = "your-aws-region") # e.g., "us-east-1"
aws_access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
region_aws <-Sys.getenv("AWS_REGION")

# Define your S3 bucket and file key
s3_bucket_name <- "new-apprentice-raw-data" # e.g., "my-data-lake"
s3_file_key <- "processed_apprentice_data/union_apprentice_data_20250527_100943.csv" # e.g., "sales/2023/monthly_sales.csv"

# --- 3. Read CSV File from S3 Bucket ---
# Using s3read_csv from the 'aws.s3' package
# This function reads the CSV directly into a data frame
tryCatch({
  df <- s3read_using(
    FUN = utils::read.csv,
    object = s3_file_key,
    bucket = s3_bucket_name,
    # Optional: If your CSV has specific read_csv arguments, add them here.
    # For example, if it's tab-separated: sep = "\t"
    # If it has a header: col_names = TRUE
    # If you need to specify column types: col_types = cols(...)
  )
  message(paste("Successfully read", s3_file_key, "from S3 bucket", s3_bucket_name))
  print("First few rows of the raw data:")
  print(head(df))
}, error = function(e) {
  detailed_error_message <- paste("Error reading CSV from S3 using s3read_using:",
                                  "Bucket:", s3_bucket_name,
                                  "Key:", s3_file_key,
                                  "Region specified:", Sys.getenv("AWS_REGION"), # Help debug region
                                  "Original error:", e$message)
   stop(detailed_error_message)
 # stop(paste("Error reading CSV from S3:", e$message))
})


# --- 4. Recode Field Records (Assign Aliases to Values within a Column) ---
# This is where you change 'r' to 'red', 'b' to 'blue', etc.

# Example 1: Recoding the 'ProductColor' column using `case_when`
# `case_when` is very flexible for multiple conditions.
df_processed <- df %>%
  mutate(
    Region = case_when(
      Region == "1" ~ "Region 1 - Boston",
      Region == "2" ~ "Region 2 - Philadelphia",
      Region == "3" ~ "Region 3 - Atlanta",
      Region == "4" ~ "Region 4 - Dallas",
      Region == "5" ~ "Region 5 - Chicago",
      Region == "6" ~ "Region 6 - San Francisco",
      Region == "8" ~ "National Programs",
      TRUE ~ as.character(Region) # Important: Keep original value if no match (e.g., for other colors)
    ),
    'Apprentice Status Code' = case_when(
      'Apprentice Status Code' == "CA" ~ "Cancelled",
      'Apprentice Status Code' == "RE" ~ "Registered",
      'Apprentice Status Code' == "CO" ~ "Completed",
      TRUE ~ as.character('Apprentice Status Code') # Important: Keep original value if no match (e.g., for other colors)
    )
    
  )

# Example 2: Recoding another hypothetical column 'Status' using `recode`
# `recode` is simpler for direct one-to-one or many-to-one mappings.
# If you had a 'Status' column with 'P' for 'Pending', 'C' for 'Completed', etc.
# df_processed <- df_processed %>%
#   mutate(
#     Status = recode(Status,
#                     "P" = "Pending",
#                     "C" = "Completed",
#                     "X" = "Cancelled"
#     )
#   )

message("Field records recoded. First few rows of the data with recoded values:")
print(head(df_processed))



# --- 5. Create a Calculation: Average Number by Year ---
# This assumes you have:
#   a) A date column (which you've potentially aliased, e.g., 'NewAliasForDate')
#   b) A numeric column (which you've potentially aliased, e.g., 'NewAliasForValue')

# First, ensure your date column is in a proper date format if it isn't already.
# If your date column is a character string, convert it.
# Example: If 'NewAliasForDate' is like "2023-01-15" or "01/15/2023"
# Adjust the format string (%Y-%m-%d, %m/%d/%Y, etc.) to match your data.
# For the format "11 12 2024 00:00:00" as discussed previously, use:
#df_renamed <- df_renamed %>%
#  mutate(
#    NewAliasForDate = mdy_hms(NewAliasForDate) # Use mdy_hms for MM DD YYYY HH:MM:SS
    # Or if your date is just "MM DD YYYY" and you want to parse it as a date:
    # NewAliasForDate = as.Date(NewAliasForDate, format = "%m %d %Y")
#  )

# First, ensure your 'Fiscal.Year' column is in a proper Date format.
# The head(df) output showed 'Fiscal.Year' as "2015-01-01", which is YYYY-MM-DD.
# If it's not already a Date type, convert it.
df_processed <- df_processed %>%
  mutate(
    Fiscal.Year = ymd(Fiscal.Year) # ymd() converts "YYYY-MM-DD" strings to Date objects
  )

# Now, extract the year and calculate the average
average_new_apprentice_by_year <- df_processed %>%
  mutate(Year = year(Fiscal.Year)) %>% # Extract year using lubridate::year()
  group_by(Region, Year) %>%
  summarise(
    TotalApprenticebyRegionandYear = sum(Total.apprentice.number, na.rm = TRUE), # Calculate mean, ignoring NAs
    #CountRecords = n() # Optional: Count records per year
  ) %>%
  ungroup() # Ungroup the data frame

message("Average value calculated by year:")
print(average_new_apprentice_by_year)


# Now, calculate YoY growth using the 'average_new_apprentice_by_year' data frame

yoy_growth_by_region_year <- average_new_apprentice_by_year %>%
  # Ensure data is sorted correctly by Region and then by Year
  # This is crucial for the lag() function to pick the correct previous year's value
  arrange(Region, Year) %>%
  
  # Group by Region, so YoY is calculated independently for each region
  group_by(Region) %>%
  
  # Create new columns for the previous year's total and the YoY growth
  mutate(
    PreviousYearTotalApprentice = lag(TotalApprenticebyRegionandYear, n = 1, order_by = Year),
    YoY_Growth_Percent = case_when(
      is.na(PreviousYearTotalApprentice) ~ NA_real_,        # No previous year data (first year for the region)
      PreviousYearTotalApprentice == 0 ~ NA_real_,          # Avoid division by zero (or return Inf, or a specific large value if meaningful)
      TRUE ~ ((TotalApprenticebyRegionandYear - PreviousYearTotalApprentice) / PreviousYearTotalApprentice) * 100
    )
  ) %>%
  
  # It's good practice to ungroup after grouped mutations if the grouping is no longer needed
  ungroup()

# Display the results
message("YoY Growth of Total Apprentices by Region and Year:")
print(yoy_growth_by_region_year)


# --- 6. (Optional) Save the processed data ---
# If you want to save the processed data locally or back to S3
# To save locally:
# write.csv(df_renamed, "processed_data.csv", row.names = FALSE)
# write.csv(average_by_year, "average_by_year.csv", row.names = FALSE)

# To save back to S3 (e.g., a new processed data bucket/path)
# s3write_csv(
#   x = df_renamed,
#   object = "path/to/processed_data.csv",
#   bucket = "your-processed-s3-bucket"
# )
