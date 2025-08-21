# This was not used in the final data project but I used this for a quick image to get the year over year growth by region. This brings in the final
# processed data from the S3 bucket and assigns alias to each regions and creates a YoY calculation

library(aws.s3)
library(dplyr)
library(lubridate) # Used for extracting year from date

# --- AWS S3 Configuration ---

aws_access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
region_aws <-Sys.getenv("AWS_REGION")

# Define your S3 bucket and file key
s3_bucket_name <- "new-apprentice-raw-data" 
s3_file_key <- "processed_apprentice_data/union_apprentice_data_20250527_100943.csv" 

# --- Read CSV File from S3 Bucket ---
tryCatch({
  df <- s3read_using(
    FUN = utils::read.csv,
    object = s3_file_key,
    bucket = s3_bucket_name,
    
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

})


# --- Recode Field Records (Assign Aliases to Values within a Column) ---
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


message("Field records recoded. First few rows of the data with recoded values:")
print(head(df_processed))



# --- Create a Calculation: Average Number by Year ---
df_processed <- df_processed %>%
  mutate(
    Fiscal.Year = ymd(Fiscal.Year) # ymd() converts "YYYY-MM-DD" strings to Date objects
  )

# Extract the year and calculate the average
average_new_apprentice_by_year <- df_processed %>%
  mutate(Year = year(Fiscal.Year)) %>% 
  group_by(Region, Year) %>%
  summarise(
    TotalApprenticebyRegionandYear = sum(Total.apprentice.number, na.rm = TRUE), # Calculate mean, ignoring NAs
  ) %>%
  ungroup() # Ungroup the data frame

message("Average value calculated by year:")
print(average_new_apprentice_by_year)


# Calculate YoY growth using the 'average_new_apprentice_by_year' data frame

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
      is.na(PreviousYearTotalApprentice) ~ NA_real_,        
      PreviousYearTotalApprentice == 0 ~ NA_real_,        
      TRUE ~ ((TotalApprenticebyRegionandYear - PreviousYearTotalApprentice) / PreviousYearTotalApprentice) * 100
    )
  ) %>%
  ungroup()

# Display the results
message("YoY Growth of Total Apprentices by Region and Year:")
print(yoy_growth_by_region_year)

