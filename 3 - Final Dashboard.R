#packages needed shiny
library(shiny)
library(shinythemes)
library(ggplot2)
library(dplyr)
library(tidyr)

#packages needed for data
library(readr)
library(lubridate)

#read csv file
data <- read_csv("/Users/jazelbarquero/Documents/R Studio Project/DataProject/NewApprentice.csv")   
#print data
print(data) 

# Prepare filter choices
state_choices <- sort(unique(data$State))
occupation_choices <- sort(unique(data$Occupation))
year_choices <- sort(unique(lubridate::year(data$`Fiscal Year`)))

#create a bar chart that shows the top 10 occupation by total number of apprentices
top_occupations <- data %>%
  group_by(Occupation) %>%
  summarise(`Total apprentice number` = n()) %>%
  arrange(desc(`Total apprentice number`)) %>%
  top_n(10, `Total apprentice number`) 
#ggplot bar chart 
top_occupation_plot <- ggplot(top_occupations, aes(x = Occupation, y = `Total apprentice number`)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Top 10 Occupations by Total Number of Apprentices", x = "Occupation", y = "Total Apprentice Number") +
  theme_minimal() 
#print top programs
print(top_occupation_plot)

#create a line chart that shows the number of apprentices by fiscal year
apprentices_by_year <- data %>%
  group_by(`Fiscal Year`) %>%
  summarise(`Total apprentice number` = n()) %>%
  arrange(`Fiscal Year`)
#ggplot line chart
apprentices_by_year_plot <- ggplot(apprentices_by_year, aes(x = `Fiscal Year`, y = `Total apprentice number`)) +
  geom_line(color = "blue", size = 1) +
  geom_point(color = "red", size = 2) +
  labs(title = "Number of Apprentices by Fiscal Year", x = "Fiscal Year", y = "Total Apprentice Number") +
  theme_minimal()
#print apprentices by year
print(apprentices_by_year_plot)
#create a detail table that shows total number of apprentices by fiscal year and state  
apprentices_detail_table <- data %>%
  group_by(`Fiscal Year`, State) %>%
  # display fiscal year as just the year
  mutate(`Fiscal Year` = year(`Fiscal Year`)) %>%     
  summarise(`Total apprentice number` = n()) %>%
  arrange(State, `Fiscal Year`) %>%
  mutate(`Fiscal Year` = as.integer(`Fiscal Year`)) # remove .00 from year      
# Print the detail table
print(apprentices_detail_table)
# Create a Shiny app to display the plots and table
ui <- fluidPage(
  theme = shinytheme("flatly"),
  titlePanel("New Apprentice Data Analysis"),
  sidebarLayout(
    sidebarPanel(
      h3("Data Summary"),
      p("This application provides insights into apprenticeship data, including the top occupations and trends over fiscal years."),
      selectInput("state", "Select State:", choices = c("All", state_choices), selected = "All"),
      selectInput("occupation", "Select Occupation:", choices = c("All", occupation_choices), selected = "All"),
      selectInput("year", "Select Fiscal Year:", choices = c("All", year_choices), selected = "All")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Top Occupations",
                 plotly::plotlyOutput("topOccupationPlot")), # changed to plotlyOutput
        tabPanel("Apprentices by Year",
                 plotly::plotlyOutput("apprenticesByYearPlot")), # changed to plotlyOutput
        tabPanel("Detail Table",
                 tableOutput("apprenticesDetailTable"))
      )
    )
  )
)
server <- function(input, output) {
  # Reactive filtered data
  filtered_data <- reactive({
    df <- data
    if (input$state != "All") {
      df <- df %>% filter(State == input$state)
    }
    if (input$occupation != "All") {
      df <- df %>% filter(Occupation == input$occupation)
    }
    if (input$year != "All") {
      df <- df %>% filter(lubridate::year(`Fiscal Year`) == as.numeric(input$year))
    }
    df
  })
  
  output$topOccupationPlot <- plotly::renderPlotly({
    top_occupations <- filtered_data() %>%
      group_by(Occupation) %>%
      summarise(`Total apprentice number` = n()) %>%
      arrange(desc(`Total apprentice number`)) %>%
      top_n(10, `Total apprentice number`)
    p <- ggplot(top_occupations, aes(y = Occupation, x = `Total apprentice number`)) +
      geom_bar(stat = "identity", fill = "steelblue") +
      labs(title = "Top 10 Occupations by Total Number of Apprentices", y = "Occupation", x = "Total Apprentice Number") +
      theme_minimal() 
    plotly::ggplotly(p, tooltip = c("y", "x"))
  })
  
  output$apprenticesByYearPlot <- plotly::renderPlotly({
    apprentices_by_year <- filtered_data() %>%
      group_by(`Fiscal Year`) %>%
      summarise(`Total apprentice number` = n()) %>%
      arrange(`Fiscal Year`)
    p <- ggplot(apprentices_by_year, aes(x = `Fiscal Year`, y = `Total apprentice number`)) +
      geom_line(color = "blue", size = 1) +
      geom_point(color = "red", size = 2) +
      labs(title = "Number of Apprentices by Fiscal Year", x = "Fiscal Year", y = "Total Apprentice Number") +
      theme_minimal()
    plotly::ggplotly(p, tooltip = c("x", "y"))
  })
  
  output$apprenticesDetailTable <- renderTable({
    apprentices_detail_table <- filtered_data() %>%
      group_by(`Fiscal Year`, State) %>%
      mutate(`Fiscal Year` = year(`Fiscal Year`)) %>%
      summarise(`Total apprentice number` = n()) %>%
      arrange(State, `Fiscal Year`) %>%
      mutate(`Fiscal Year` = as.integer(`Fiscal Year`)) # remove .00 from year
    apprentices_detail_table
  })
}           
# Run the Shiny app
shinyApp(ui = ui, server = server)
# Save the app as an R script file
# saveRDS(app, file = "NewApprenticeApp.rds")
# Note: The above line is commented out because saving the app as an RDS file is
# not necessary for running the app. The app can be run directly from this script.
# To run the app, simply source this script in RStudio or run it in an R environment.
# The app will automatically launch in a web browser.
# Ensure that the necessary packages are installed and loaded before running the app.
# You can install any missing packages using install.packages("package_name").