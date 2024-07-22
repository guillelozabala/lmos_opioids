library(tidyverse)
library(fixest)
library(ggtext)
library(binsreg)
library(gridExtra)
library(grid)

rm(list = ls())
set.seed(123)
options(max.print = 250)

# Set the path
file_location <- rstudioapi::getSourceEditorContext()$path
setwd(dirname(file_location)) # set path to location

# Auxiliary functions
source(paste(getwd(), "utilities.R", sep = "/"))
source(paste(getwd(), "plots.R", sep = "/"))

# Percentiles of the wage distribution
percentiles <- c("0.10", "0.25", "0.50", "0.75", "0.90")

# Load data's location
data_location_pmq <- "C:/Users/g-mart36/Documents/GitHub/lmos_opioids/data/processed/merged_data.csv"

# Load the data for Must Query PDMPs
df_pmq <- read.csv(
  data_location_pmq,
  header = TRUE,
  sep = ","
)

# Include relative-to-treatement time markers and presc per capita
df_pmq <- df_pmq |>
  dplyr::mutate(
    time_marker = (year - 1960) * 12 + month,
    relative_to_treat_pmq = time_marker - first_treatment_pmq#,
    #dosage_unit_pc = round(dosage_unit / population, 3)
  )

# Select the covariate columns
names_covar_pmq <- names(
  df_pmq[, c(grep("_ratio$", colnames(df_pmq)),
        grep("model_values", colnames(df_pmq)))]
)

# Prepare the covariates (as formula)
covariates_pmq <- paste(
  names_covar_pmq,
  collapse = " + "
)

unem_rate_pmq_effects  <- df_pmq |>
  indiv_effects(
    yname = "unemployment_rate",
    iname = "fips",
    tname = "time_marker",
    kname = "relative_to_treat_pmq",
    aname = "first_treatment_pmq",
    covariates = covariates_pmq,
    only_full_horizon = FALSE
  )

lab_force_rate_pmq_effects  <- df_pmq |>
  indiv_effects(
    yname = "lab_force_rate",
    iname = "fips",
    tname = "time_marker",
    kname = "relative_to_treat_pmq",
    aname = "first_treatment_pmq",
    covariates = covariates_pmq,
    only_full_horizon = FALSE
  )

# Set plot locations
pmq_unrate_10_loc <- paste(location, "slides/pmq10_unemp_rate.png", sep = "/")
pmq_lfrate_10_loc <- paste(location, "slides/pmq10_lab_for_rate.png", sep = "/")

pmq_unrate_plot_df <- unem_rate_pmq_effects$df_indcp
pmq_lfrate_plot_df <- lab_force_rate_pmq_effects$df_indcp

# Display and save the results for participation rates
png(filename = pmq_lfrate_10_loc, width = 600, height = 539)
ind_effects_plot(
  pmq_lfrate_plot_df,
  c(1, 6, 12, 24),
  percentiles[1]
)
dev.off()

# Display and save the results for participation rates
png(filename = pmq_unrate_10_loc, width = 600, height = 539)
ind_effects_plot(
  pmq_unrate_plot_df,
  c(1, 6, 12, 24),
  percentiles[1]
)
dev.off()


data_states_location <- "C:/Users/g-mart36/Documents/GitHub/lmos_opioids/data/processed/merged_data_states.csv"

# Load the data -- State-level data
df_pmq_states <- read.csv(
  data_states_location,
  header = TRUE,
  sep = ","
)

# Change in python file
df_pmq_states <- df_pmq_states %>%
  mutate(, as.numeric))

# Include relative-to-treatement time markers and presc per capita
df_pmq_states <- df_pmq_states |>
  dplyr::mutate(
    time_marker = (year - 1960) * 12 + month,
    relative_to_treat_pmq = time_marker - first_treatment_pmq #,
    #dosage_unit_pc = round(dosage_unit / population, 3)
)

# Select the covariate columns
names_covar_pmq_states <- names(
  df_pmq_states[, c(
    grep("_ratio$", colnames(df_pmq_states)),
    grep("model_values", colnames(df_pmq_states))
  )]
)

# Prepare the covariates (as formula)
covariates_pmq_states <- paste(
  names_covar_pmq_states,
  collapse = " + "
)

unem_rate_pmq_effects_states  <- df_pmq_states |>
  indiv_effects(
    yname = "unemployment_rate",
    iname = "state_fip",
    tname = "time_marker",
    kname = "relative_to_treat_pmq",
    aname = "first_treatment_pmq",
    covariates = covariates_pmq,
    only_full_horizon = FALSE
  )

lab_force_rate_pmq_effects_states  <- df_pmq_states |>
  indiv_effects(
    yname = "lab_force_rate",
    iname = "state_fip",
    tname = "time_marker",
    kname = "relative_to_treat_pmq",
    aname = "first_treatment_pmq",
    covariates = covariates_pmq,
    only_full_horizon = FALSE
  )

ind_effects_plot(
  pmq_lfrate_plot_df,
  c(1, 6, 12, 24),
  percentiles[1]
)




# Time averages

# Set plot locations
pmq_unrate_10_ta_loc <- paste(location, "slides/pmq10_unemp_rate_ta_states.png", sep = "/")
pmq_lfrate_10_ta_loc <- paste(location, "slides/pmq10_lab_for_rate_ta_states.png", sep = "/")

# Display and save the results for participation rates
png(filename = pmq_lfrate_10_ta_loc, width = 600, height = 539)
TimeAveragesPlot(
  pmq_lfrate_plot_df,
  percentiles[1],
  24,
  with_data = FALSE
)
dev.off()

# Display and save the results for unemployment rates
png(filename = pmq_unrate_10_ta_loc, width = 600, height = 539)
TimeAveragesPlot(
  pmq_unrate_plot_df,
  percentiles[1],
  24,
  with_data = FALSE
)
dev.off()

# Display and save the results for employment rates
png(filename = pmq_emrate_10_ta_loc, width = 600, height = 539)
TimeAveragesPlot(
  pmq_emrate_plot_df,
  percentiles[1],
  24,
  with_data = FALSE
)
dev.off()

# Compare percentiles

# Set plot locations
pmq_lfrate_10_comp6_loc <- paste(location, "slides/pmq10_lab_for_rate_comp6.png", sep = "/")
pmq_lfrate_10_comp24_loc <- paste(location, "slides/pmq10_lab_for_rate_comp24.png", sep = "/")
pmq_unrate_10_comp6_loc <- paste(location, "slides/pmq10_unemp_rate_comp6.png", sep = "/")
pmq_unrate_10_comp24_loc <- paste(location, "slides/pmq10_unemp_rate_comp24.png", sep = "/")
pmq_emrate_10_comp6_loc <- paste(location, "slides/pmq10_emp_rate_comp6.png", sep = "/")
pmq_emrate_10_comp24_loc <- paste(location, "slides/pmq10_emp_rate_comp24.png", sep = "/")

# Display and save the results for participation rates
png(filename = pmq_lfrate_10_comp6_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_lfrate_plot_df,
    6,
    percentiles
)
dev.off()

png(filename = pmq_lfrate_10_comp24_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_lfrate_plot_df,
    24,
    percentiles
)
dev.off()

# Display and save the results for unemployment rates
png(filename = pmq_unrate_10_comp6_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_unrate_plot_df,
    6,
    percentiles
)
dev.off()

png(filename = pmq_unrate_10_comp24_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_unrate_plot_df,
    24,
    percentiles
)
dev.off()

# Display and save the results for employment rates
png(filename = pmq_emrate_10_comp6_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_emrate_plot_df,
    6,
    percentiles
)
dev.off()

png(filename = pmq_emrate_10_comp24_loc, width = 600, height = 539)
IndividualEffectsCompPlot(
    pmq_emrate_plot_df,
    24,
    percentiles
)
dev.off()

# Controlling for prescriptions

# Select the covariate columns
names_covar_pmq_p <- names(
  df_pmq[, c(grep("_ratio$", colnames(df_pmq)),
        grep("model_values", colnames(df_pmq)),
        grep("dosage_unit_pc", colnames(df_pmq)))]
  )
# Prepare the covariates (as formula)
covariates_pmq_p <- paste(
  names_covar_pmq_p,
  collapse = " + "
  )

# Compute the coefficient values
unem_rate_pmq_effects_p  <- df_pmq |>
  indiv_effects(yname = "unem_rate",
            iname = "fips",
            tname = "time_marker",
            kname = "relative_to_treat_pmq",
            aname = "first_treatment_pmq",
            covariates = covariates_pmq_p,
            only_full_horizon = FALSE)

emp_rate_pmq_effects_p  <- df_pmq |>
  indiv_effects(yname = "emp_rate",
            iname = "fips",
            tname = "time_marker",
            kname = "relative_to_treat_pmq",
            aname = "first_treatment_pmq",
            covariates = covariates_pmq_p,
            only_full_horizon = FALSE)

lab_force_rate_pmq_effects_p  <- df_pmq |>
  indiv_effects(yname = "lab_force_rate",
            iname = "fips",
            tname = "time_marker",
            kname = "relative_to_treat_pmq",
            aname = "first_treatment_pmq",
            covariates = covariates_pmq_p,
            only_full_horizon = FALSE)

# Set plot locations
pmq_unrate_10_loc_p <- paste(location, "slides/pmq10_unemp_rate_p.png", sep = "/")
pmq_emrate_10_loc_p <- paste(location, "slides/pmq10_emp_rate_p.png", sep = "/")
pmq_lfrate_10_loc_p <- paste(location, "slides/pmq10_lab_for_rate_p.png", sep = "/")

# Set plot datasets
pmq_unrate_plot_df_p <- unem_rate_pmq_effects_p$df_indcp
pmq_emrate_plot_df_p <- emp_rate_pmq_effects_p$df_indcp
pmq_lfrate_plot_df_p <- lab_force_rate_pmq_effects_p$df_indcp

# Display and save the results for unemployment rates
png(filename = pmq_unrate_10_loc_p, width = 600, height = 539)
IndividualEffectsPlot(
  pmq_unrate_plot_df_p,
  c(1, 12),
  percentiles[1]
  )
dev.off()

# Display and save the results for employment rates
png(filename = pmq_emrate_10_loc_p, width = 600, height = 539)
IndividualEffectsPlot(
  pmq_emrate_plot_df_p,
  c(1, 12),
  percentiles[1]
  )
dev.off()

# Display and save the results for participation rates
png(filename = pmq_lfrate_10_loc_p, width = 600, height = 539)
IndividualEffectsPlot(
  pmq_lfrate_plot_df_p,
  c(1, 12),
  percentiles[1]
  )
dev.off()

# Overdose deaths

# Load data's location
data_location_opioid_deaths <- paste(
  location,
  "data/processed/opioid_deaths_df.csv",
  sep = "/"
  )

# Load the data for Must Query PDMPs
df_opioid_deaths <- read.csv(
  data_location_opioid_deaths,
  header = TRUE,
  sep = ","
  )

# Include relative-to-treatement time markers and presc per capita
df_opioid_deaths <- df_opioid_deaths |>
  dplyr::mutate(
    relative_to_treat_pmq = year - pmq_year,
    deaths_pc = round((deaths / population) * 100000, 3)
  )

# Select the covariate columns
opioid_deaths_covar_pmq <- names(
  df_opioid_deaths[, grep("_ratio$", colnames(df_opioid_deaths))]
  )

# Prepare the covariates (as formula)
opioid_deaths_covariates_pmq <- paste(
  opioid_deaths_covar_pmq,
  collapse = " + "
  )

opioids_pmq_effects  <- df_opioid_deaths |>
  indiv_effects(yname = "deaths_pc",
            iname = "state_code",
            tname = "year",
            kname = "relative_to_treat_pmq",
            aname = "pmq_year",
            covariates = opioid_deaths_covariates_pmq,
            only_full_horizon = FALSE)

opioids_pmq_effects <- opioids_pmq_effects$df_indcp

df_heroin_deaths <- df_opioid_deaths[df_opioid_deaths$cause_of_death == "Heroin", ]

heroin_pmq_effects  <- df_heroin_deaths |>
  indiv_effects(yname = "deaths_pc",
            iname = "state_code",
            tname = "year",
            kname = "relative_to_treat_pmq",
            aname = "pmq_year",
            covariates = opioid_deaths_covariates_pmq,
            only_full_horizon = FALSE)

heroin_pmq_effects <- heroin_pmq_effects$df_indcp
heroin_time_avg <- heroin_pmq_effects[heroin_pmq_effects$relative_to_treat_pmq >= 1, ]

heroin_time_avg <- heroin_time_avg |>
  group_by(state_code) |>
  summarize(
    deaths_pc_tilde_mean = mean(deaths_pc_tilde, na.rm = TRUE),
    kaitz_pct10_mean = mean(kaitz_pct10, na.rm = TRUE))

summary(lm(deaths_pc_tilde_mean ~ kaitz_pct10_mean, heroin_time_avg))

heroin_time_avg_plot <- ggplot(data = heroin_time_avg) +
  geom_point(
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    size = 4
    ) +
  geom_smooth(
    method = "lm",
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    se = FALSE,
    color = "#0098e9"
    ) +
  ylab("") +
  xlab("") +
  labs(title = "Heroin") +
  geom_hline(
    yintercept = 0,
    linetype = 5,
    color = "#000000",
    linewidth = 0.5
    )

heroin_time_avg_plot <- heroin_time_avg_plot +
    theme_classic() +
    theme(
      text = element_text(size = 12, family = "serif"),
      plot.title = element_text(hjust = 0.5)
    )

df_methadone_deaths <- df_opioid_deaths[df_opioid_deaths$cause_of_death == "Methadone",]

methadone_pmq_effects  <- df_methadone_deaths |>
  indiv_effects(yname = "deaths_pc",
            iname = "state_code",
            tname = "year",
            kname = "relative_to_treat_pmq",
            aname = "pmq_year",
            covariates = opioid_deaths_covariates_pmq,
            only_full_horizon = FALSE)

methadone_pmq_effects <- methadone_pmq_effects$df_indcp
methadone_time_avg <- methadone_pmq_effects[methadone_pmq_effects$relative_to_treat_pmq >= 1, ]

methadone_time_avg <- methadone_time_avg |>
  group_by(state_code) |>
  summarize(
    deaths_pc_tilde_mean = mean(deaths_pc_tilde, na.rm = TRUE),
    kaitz_pct10_mean = mean(kaitz_pct10, na.rm = TRUE))

summary(lm(deaths_pc_tilde_mean ~ kaitz_pct10_mean, methadone_time_avg))

methadone_time_avg_plot <- ggplot(data = methadone_time_avg) +
  geom_point(
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    size = 4
    ) +
  geom_smooth(
    method = "lm",
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    se = FALSE,
    color = "#0098e9"
    ) +
  ylab("") +
  xlab("") +
  labs(title = "Methadone") +
  geom_hline(
    yintercept = 0,
    linetype = 5,
    color = "#000000",
    linewidth = 0.5
    )

methadone_time_avg_plot <- methadone_time_avg_plot +
    theme_classic() +
    theme(
      text = element_text(size = 12, family = "serif"),
      plot.title = element_text(hjust = 0.5)
    )

df_otheropioids_deaths <- df_opioid_deaths[df_opioid_deaths$cause_of_death == "Other opioids",]

otheropioids_pmq_effects  <- df_otheropioids_deaths |>
  indiv_effects(yname = "deaths_pc",
            iname = "state_code",
            tname = "year",
            kname = "relative_to_treat_pmq",
            aname = "pmq_year",
            covariates = opioid_deaths_covariates_pmq,
            only_full_horizon = FALSE)

otheropioids_pmq_effects <- otheropioids_pmq_effects$df_indcp
otheropioids_time_avg <- otheropioids_pmq_effects[otheropioids_pmq_effects$relative_to_treat_pmq >= 1, ]

otheropioids_time_avg <- otheropioids_time_avg |>
  group_by(state_code) |>
  summarize(
    deaths_pc_tilde_mean = mean(deaths_pc_tilde, na.rm = TRUE),
    kaitz_pct10_mean = mean(kaitz_pct10, na.rm = TRUE))

summary(lm(deaths_pc_tilde_mean ~ kaitz_pct10_mean, otheropioids_time_avg))

otheropioids_time_avg_plot <- ggplot(data = otheropioids_time_avg) +
  geom_point(
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    size = 4
    ) +
  geom_smooth(
    method = "lm",
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    se = FALSE,
    color = "#0098e9"
    ) +
  ylab("") +
  xlab("") +
  labs(title = "Other opioids") +
  geom_hline(
    yintercept = 0,
    linetype = 5,
    color = "#000000",
    linewidth = 0.5
    )

otheropioids_time_avg_plot <- otheropioids_time_avg_plot +
    theme_classic() +
    theme(
      text = element_text(size = 12, family = "serif"),
      plot.title = element_text(hjust = 0.5)
    )

df_synopioids_deaths <- df_opioid_deaths[df_opioid_deaths$cause_of_death == "Other synthetic narcotics",]

synopioids_pmq_effects  <- df_synopioids_deaths |>
  indiv_effects(yname = "deaths_pc",
            iname = "state_code",
            tname = "year",
            kname = "relative_to_treat_pmq",
            aname = "pmq_year",
            covariates = opioid_deaths_covariates_pmq,
            only_full_horizon = FALSE)

synopioids_pmq_effects <- synopioids_pmq_effects$df_indcp
synopioids_time_avg <- synopioids_pmq_effects[synopioids_pmq_effects$relative_to_treat_pmq >= 1, ]

synopioids_time_avg <- synopioids_time_avg |>
  group_by(state_code) |>
  summarize(
    deaths_pc_tilde_mean = mean(deaths_pc_tilde, na.rm = TRUE),
    kaitz_pct10_mean = mean(kaitz_pct10, na.rm = TRUE))

summary(lm(deaths_pc_tilde_mean ~ kaitz_pct10_mean, synopioids_time_avg))

synopioids_time_avg_plot <- ggplot(data = synopioids_time_avg) +
  geom_point(
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    size = 4
    ) +
  geom_smooth(
    method = "lm",
    aes(x = kaitz_pct10_mean, y = deaths_pc_tilde_mean),
    se = FALSE,
    color = "#0098e9"
    ) +
  ylab("") +
  xlab("") +
  labs(title = "Other synthetic narcotics") +
  geom_hline(
    yintercept = 0,
    linetype = 5,
    color = "#000000",
    linewidth = 0.5
    )

synopioids_time_avg_plot <- synopioids_time_avg_plot +
      theme_classic() +
      theme(
        text = element_text(size = 12, family = "serif"),
        plot.title = element_text(hjust = 0.5)
      )

setting_labs_opioids <- gpar(fontsize = 17, fontfamily = "serif")
setting_title_opioids <- gpar(fontsize = 20, fontfamily = "serif")

lab_left_opioids <- textGrob(
    "Individual effects",
    rot = 90,
    gp = setting_labs_opioids
    )

lab_bottom_opioids <- textGrob(
    "Kaitz-p values",
    gp = setting_labs_opioids
    )

lab_top_opioids <- textGrob(
    paste0("Effects of Must Query PDMPs on overdose deaths\n by minimum wage bindingness (Kaitz-0.10 index),\n time averages"),
    gp = setting_title_opioids
    )

pmq_heroin_10_loc <- paste(location, "slides/pmq10_heroin.png", sep = "/")
png(filename = pmq_heroin_10_loc, width = 600, height = 539)
grid.arrange(
    grobs = list(
        heroin_time_avg_plot,
        methadone_time_avg_plot,
        otheropioids_time_avg_plot,
        synopioids_time_avg_plot
        ),
    nrow = 2,
    left = lab_left_opioids,
    bottom = lab_bottom_opioids,
    top = lab_top_opioids
    )
dev.off()

# Prescriptions

data_location_prescriptions_pmq <- paste(
  location,
  "data/processed/prescription_monthly_pmq.csv",
  sep = "/"
  )

df_prescriptions_pmq <- read.csv(
  data_location_prescriptions_pmq,
  header = TRUE,
  sep = ","
  )

names_covar_prescriptions_pmq <- names(
  df_prescriptions_pmq[, c(grep("_ratio$", colnames(df_prescriptions_pmq)),
  grep("scaled_model_values", colnames(df_prescriptions_pmq)))]
  )

covariates_prescriptions_pmq <- paste(
  names_covar_prescriptions_pmq,
  collapse = " + "
  )

df_prescriptions_pmq[['relative_to_treat_pmq']] = df_prescriptions_pmq[['year']] - df_prescriptions_pmq[['pmq_year']]

prescriptions_pmq_effects  <- df_prescriptions_pmq |>
  indiv_effects(yname = "dosage_unit_pc",
            iname = "fips",
            tname = "time_marker",
            kname = "relative_to_treat_pmq",
            aname = "first_treatment_pmq",
            covariates = covariates_prescriptions_pmq,
            only_full_horizon = FALSE)


png(filename = paste(location,"slides/pmq10_prescriptions.png",sep = "/"),width = 600, height = 539)
IndividualEffectsPlot(prescriptions_pmq_effects$df_indcp, c(1,2), percentiles[1])
dev.off()
