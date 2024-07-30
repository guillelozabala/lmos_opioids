kait_densities_plot <- function(df) {

  # Define the plot title
  plot_title <- "Distribution of Kaitz-p indices across counties,
  for different \n percentiles at treatment (Must Query PDMP)"

  # Get the column names of the dataframe
  df_colnames <- df |> colnames()

  # Filter the dataframe to include only observations at treatment
  rel_to_treat_name <- df_colnames[grep("relative_to_treat_", df_colnames)]
  df <- df[df[[rel_to_treat_name]] == 0, ]

  # Define the colors for the density plots
  density_colors <- c(
    "0.10" = "#0098e9",
    "0.25" = "#ff5ca8",
    "0.50" = "#5aa800",
    "0.75" = "#f29318",
    "0.90" = "#B79F00"
  )

  # Create the densities plot
  densities_plot <- ggplot2::ggplot(df) +
    geom_density(
      aes(x = kaitz_pct10, ..count.. / sum(..count..), color = "0.10"),
      linewidth = 1
    ) +
    geom_density(
      aes(x = kaitz_pct25, ..count.. / sum(..count..), color = "0.25"),
      linewidth = 1
    ) +
    geom_density(
      aes(x = kaitz_pct50, ..count.. / sum(..count..), color = "0.50"),
      linewidth = 1
    ) +
    geom_density(
      aes(x = kaitz_pct75, ..count.. / sum(..count..), color = "0.75"),
      linewidth = 1
    ) +
    geom_density(
      aes(x = kaitz_pct90, ..count.. / sum(..count..), color = "0.90"),
      linewidth = 1
    ) +
    scale_color_manual(
      name = element_blank(),
      values = density_colors
    )  +
    ylab(
      ""
    ) +
    xlab(
      "Kaitz-p index"
    ) +
    labs(
      title = plot_title
    )

  # Customize the plot appearance
  densities_plot <- densities_plot +
    theme_classic() +
    theme(
      text = element_text(size = 16, family = "serif"),
      plot.title = element_text(hjust = 0.5, lineheight = 1.1),
      legend.position = c(0.10, 0.80)
    )

  return(densities_plot)

}

title_and_colors <- function(outcome, percentile) {

  # Define the possible percentiles
  percentiles <- c("0.10", "0.25", "0.50", "0.75", "0.90")

  if (percentile %in% percentiles) {
    # Define the Kaitz-p index percentile
    percentile_name <- paste0("kaitz_pct", gsub("0.", "", percentile))
  } else {
    print("Percentile must be 0.10, 0.25, 0.50, 0.75 or 0.90")
  }

  if (outcome == "unemployment_rate") {
    outcome_title <- "unemployment rate"
    plot_color <- "#0098e9"
    plot_yrange <- ylim(-1.5, 4.0)
  } else if (outcome == "emp_rate") {
    outcome_title <- "employment rate"
    plot_color <- "#ff5ca8"
    plot_yrange <- ylim(-4.0, 1.5)
  } else if (outcome == "lab_force_rate") {
    outcome_title <- "labor force rate"
    plot_color <- "#5aa800"
    plot_yrange <- ylim(-3.0, 3.5)
  } else if (outcome == "model_values") {
    outcome_title <- "mortality rate"
    plot_color <- "#0098e9"
    plot_yrange <- ylim(-0.01, 0.01)
  } else if (outcome == "dosage_unit_pc") {
    outcome_title <- "prescriptions per capita"
    plot_color <- "#0098e9"
    plot_yrange <- ylim(-1.5, 0.5)
  } else if (outcome == "deaths_pc") {
    outcome_title <- "opioid deaths_pc"
    plot_color <- "#0098e9"
    plot_yrange <- ylim(-3.0, 3.0)
  }

  # Store the naming details in a vector
  titles_and_colors <- c(
    percentile_name,
    plot_color,
    plot_yrange,
    outcome_title
  )

  return(titles_and_colors)

}

ind_effects_plot <- function(df, periods, percentile) {

  # Get the column names of the dataframe
  df_colnames <- df |> colnames()

  # Obtain the policy from treatment name
  #policy_name <- "first_treatment_pmq"
  policy <- "pmq"

  # Obtain the outcome from dependend name
  outcome_name <- df_colnames[grep("_tilde", colnames(df))]
  outcome <- sub("_tilde", "", outcome_name)

  # Get the naming details for the plot
  naming <- title_and_colors(outcome, percentile)

  # Extract the naming details
  percentile_name <- naming[[1]]
  plot_color <- naming[[2]]
  plot_yrange <- naming[[3]]
  policy_title <- "Must Query PDMPs"
  outcome_title <- naming[[4]]

  # Create the plot title
  plot_title <- paste0(
    "Effects of ",
    policy_title,
    " on ",
    outcome_title,
    "\n by minimum wage bindingness (Kaitz-",
    percentile,
    " index)"
  )

  # Create an empty list to store the plots
  plot_collection <- list()
  bins_collection <- list()

  for (period in periods){

    # Select the data for the specific period and policy
    plot_data <- df[df[["relative_to_treat_pmq"]] == period, ]

    # Perform binscatter regression
    binscatter <- binsreg::binsreg(
      y = plot_data[[outcome_name]],
      x = plot_data[[percentile_name]],
      data = plot_data,
      ci = TRUE
    )

    # Extract the binscatter data
    binscatter_data <- binscatter$data.plot$`Group Full Sample`$data.bin

    # Create an empty matrix to store the results
    bins_plot_df <- matrix(0, nrow(binscatter_data), 4)

    # Iterate over each bin
    for (i in seq_len(nrow(binscatter_data))) {
      # Get the left and right endpoints of the bin
      bin_lside <- binscatter_data[["left.endpoint"]][i]
      bin_rside <- binscatter_data[["right.endpoint"]][i]

      # Select the data within the bin
      l_hand_side <- plot_data[[percentile_name]] >= bin_lside
      r_hand_side <- plot_data[[percentile_name]] < bin_rside
      c_interv <- plot_data[(l_hand_side) & (r_hand_side), ]

      # Calculate the mean and confidence interval for the bin
      bins_plot_df[i, 1] <- mean(c_interv[[outcome_name]], na.rm = TRUE)
      within_bin_var <- var(c_interv[[outcome_name]], na.rm = TRUE)
      interv_scope <- qnorm(0.025) * sqrt(within_bin_var / nrow(c_interv))
      bins_plot_df[i, 2] <- bins_plot_df[i, 1] - interv_scope
      bins_plot_df[i, 3] <- bins_plot_df[i, 1] + interv_scope
    }

    # Add the x values to the bins_plot_df matrix
    bins_plot_df[, 4] <- binscatter$data.plot$`Group Full Sample`$data.dots$x

    # Convert the bins_plot_df matrix to a tibble for easier manipulation
    bins_plot_df <- tibble::as_tibble(bins_plot_df)

    # Rename the columns of the tibble
    names(bins_plot_df) <- c("fit", "ci.l", "ci.r", "x")

    # Create the time_period variable for the plot title
    time_period <- paste0("t = ", gsub("V", "", period))

    # Create the binscatter plot
    plot <- ggplot2::ggplot(
      bins_plot_df,
      aes(x = x, y = fit)
      ) +
      geom_point(
        color = plot_color
      ) +
      geom_line(
        color = plot_color
      ) +
      geom_errorbar(
        aes(ymin = ci.l, ymax = ci.r),
        color = plot_color
      ) +
      plot_yrange +
      xlim(-0.6, -0.2) +
      ylab(
        ""
      ) +
      xlab(
        ""
      ) +
      labs(
        title = time_period
      ) +
      geom_hline(
        yintercept = 0,
        linetype = 5,
        color = "#000000",
        linewidth = 0.5
    )

    # Add the theme to the plot
    plot <- plot +
      theme_classic() +
      theme(
        text = element_text(size = 12, family = "serif"),
        plot.title = element_text(hjust = 0.5)
      )

    # Store the plot
    plot_collection[[paste0("plot_", period)]] <- plot
    bins_collection[[paste0("bins_", period)]] <- bins_plot_df

  }

  # If only one period is selected, return a single plot
  if (length(periods) == 1) {
    # Select the plot
    single_plot <- plot_collection[[paste0("plot_", periods)]]
    # Adapt aesthetics
    single_plot <- single_plot +
      ylab(
        "Individual effects"
      ) +
      xlab(
        "Kaitz-p values"
      ) +
      labs(
        title = plot_title
      )
    return(single_plot)
  }

  # Set the font size and family for the labels and title
  setting_labs <- grid::gpar(fontsize = 17, fontfamily = "serif")
  setting_title <- grid::gpar(fontsize = 20, fontfamily = "serif")

  # Create a textGrob for the label on the left side of the plot
  lab_left <- grid::textGrob("Individual effects", rot = 90, gp = setting_labs)

  # Create a textGrob for the label on the bottom of the plot
  lab_bottom <- grid::textGrob("Kaitz-p values", gp = setting_labs)

  # Create a textGrob for the title of the plot
  lab_top <- grid::textGrob(plot_title, gp = setting_title)

  # Arrange the plots in a grid layout with 2 columns
  plot_grid <- gridExtra::grid.arrange(
    grobs = plot_collection,
    ncol = 2,
    left = lab_left,
    bottom = lab_bottom,
    top = lab_top
  )

  return(c(plot_grid, bins_collection))

}

ind_effects_plot_states <- function(df, periods, percentile) {

  # Get the column names of the dataframe
  df_colnames <- df |> colnames()

  # Obtain the outcome from dependend name
  outcome_name <- df_colnames[grep("_tilde", colnames(df))]
  outcome <- sub("_tilde", "", outcome_name)

  # Get the naming details for the plot
  naming <- title_and_colors(outcome, percentile)

  # Extract the naming details
  percentile_name <- naming[[1]]
  plot_color <- naming[[2]]
  plot_yrange <- naming[[3]]
  policy_title <- "Must Query PDMPs"
  outcome_title <- naming[[4]]

  # Create the plot title
  plot_title <- paste0(
    "Effects of ",
    policy_title,
    " on ",
    outcome_title,
    "\n by minimum wage bindingness (Kaitz-",
    percentile,
    " index)"
  )

  # Create an empty list to store the plots
  plot_collection <- list()
  bins_collection <- list()

  for (period in periods){

    # Select the data for the specific period and policy
    plot_data <- df[df[["relative_to_treat_pmq"]] == period, ]

    # Perform binscatter regression
    binscatter <- binsreg::binsreg(
      y = plot_data[[outcome_name]],
      x = plot_data[["unemployed_per_job_opening_ratio_rate"]],
      data = plot_data,
      ci = TRUE
    )

    # Extract the binscatter data
    binscatter_data <- binscatter$data.plot$`Group Full Sample`$data.bin

    # Create an empty matrix to store the results
    bins_plot_df <- matrix(0, nrow(binscatter_data), 4)

    # Iterate over each bin
    for (i in seq_len(nrow(binscatter_data))) {
      # Get the left and right endpoints of the bin
      bin_lside <- binscatter_data[["left.endpoint"]][i]
      bin_rside <- binscatter_data[["right.endpoint"]][i]

      # Select the data within the bin
      l_hand_side <- plot_data[['unemployed_per_job_opening_ratio_rate']] >= bin_lside
      r_hand_side <- plot_data[['unemployed_per_job_opening_ratio_rate']] <= bin_rside
      c_interv <- plot_data[(l_hand_side) & (r_hand_side), ]

      # Calculate the mean and confidence interval for the bin
      bins_plot_df[i, 1] <- mean(c_interv[[outcome_name]], na.rm = TRUE)
      within_bin_var <- var(c_interv[[outcome_name]], na.rm = TRUE)
      interv_scope <- qnorm(0.025) * sqrt(within_bin_var / nrow(c_interv))
      bins_plot_df[i, 2] <- bins_plot_df[i, 1] - interv_scope
      bins_plot_df[i, 3] <- bins_plot_df[i, 1] + interv_scope
    }

    # Add the x values to the bins_plot_df matrix
    bins_plot_df[, 4] <- binscatter$data.plot$`Group Full Sample`$data.dots$x

    # Convert the bins_plot_df matrix to a tibble for easier manipulation
    bins_plot_df <- tibble::as_tibble(bins_plot_df)

    # Rename the columns of the tibble
    names(bins_plot_df) <- c("fit", "ci.l", "ci.r", "x")

    # Create the time_period variable for the plot title
    time_period <- paste0("t = ", gsub("V", "", period))

    # Create the binscatter plot
    plot <- ggplot2::ggplot(
      bins_plot_df,
      aes(x = x, y = fit)
      ) +
      geom_point(
        color = plot_color
      ) +
      geom_line(
        color = plot_color
      ) +
      geom_errorbar(
        aes(ymin = ci.l, ymax = ci.r),
        color = plot_color
      ) +
      plot_yrange +
      xlim(0.5, 2.0) +
      ylab(
        ""
      ) +
      xlab(
        ""
      ) +
      labs(
        title = time_period
      ) +
      geom_hline(
        yintercept = 0,
        linetype = 5,
        color = "#000000",
        linewidth = 0.5
    )

    # Add the theme to the plot
    plot <- plot +
      theme_classic() +
      theme(
        text = element_text(size = 12, family = "serif"),
        plot.title = element_text(hjust = 0.5)
      )

    # Store the plot
    plot_collection[[paste0("plot_", period)]] <- plot
    bins_collection[[paste0("bins_", period)]] <- bins_plot_df

  }

  # If only one period is selected, return a single plot
  if (length(periods) == 1) {
    # Select the plot
    single_plot <- plot_collection[[paste0("plot_", periods)]]
    # Adapt aesthetics
    single_plot <- single_plot +
      ylab(
        "Individual effects"
      ) +
      xlab(
        "unemployed_per_job_opening_ratio_rate"
      ) +
      labs(
        title = plot_title
      )
    return(single_plot)
  }

  # Set the font size and family for the labels and title
  setting_labs <- grid::gpar(fontsize = 17, fontfamily = "serif")
  setting_title <- grid::gpar(fontsize = 20, fontfamily = "serif")

  # Create a textGrob for the label on the left side of the plot
  lab_left <- grid::textGrob("Individual effects", rot = 90, gp = setting_labs)

  # Create a textGrob for the label on the bottom of the plot
  lab_bottom <- grid::textGrob("unemployed_per_job_opening_ratio_rate", gp = setting_labs)

  # Create a textGrob for the title of the plot
  lab_top <- grid::textGrob(plot_title, gp = setting_title)

  # Arrange the plots in a grid layout with 2 columns
  plot_grid <- gridExtra::grid.arrange(
    grobs = plot_collection,
    ncol = 2,
    left = lab_left,
    bottom = lab_bottom,
    top = lab_top
  )

  return(c(plot_grid, bins_collection))

}


TimeAveragesPlot <- function(df, percentile, up_to, with_data = FALSE) {

  # Get the column names of the dataframe
  df_colnames <- df |> colnames()

  # Obtain the policy from treatment name
  policy_name <- df_colnames[grep("first_treatment_", colnames(df))]
  policy <- sub("first_treatment_", "", policy_name)

  # Obtain the outcome from dependend name
  outcome_name <- df_colnames[grep("_tilde", colnames(df))]
  outcome <- sub("_tilde", "", outcome_name)

  # Obtain relative-to-treatment time
  rel_treat_name <- df_colnames[grep("relative_to_treat_", colnames(df))]

  # Get the naming details for the plot
  naming <- TitleAndColors(policy, outcome, percentile)

  # Extract the naming details
  percentile_name <- naming[[1]]
  plot_color <- naming[[2]]
  plot_yrange <- naming[[3]]
  policy_title  <- naming[[4]]
  outcome_title <- naming[[5]]

  # Create the plot title
  plot_title <- paste0(
    "Effects of ",
    policy_title,
    " on ",
    outcome_title,
    "\n by minimum wage bindingness (Kaitz-",
    percentile,
    " index),\n time average"
  )

  # Select the subset of data based on the relative-to-treatment values
  df_short <- df[dplyr::between(df[[rel_treat_name]], 1, up_to), ]

  # Calculate the average of the outcome variable for each group
  df_plot <- df_short |>
    dplyr::group_by(fips) |>
    dplyr::summarize(average = mean(!!rlang::sym(outcome_name), na.rm = TRUE))

  # Select the relevant columns for the Kaitz-p values
  df_short_kaitz <- df_short[, c("fips", percentile_name, rel_treat_name)]

  # Merge the average data with the Kaitz-p values data
  df_plot <- merge(
    df_plot,
    df_short_kaitz[df_short_kaitz[[rel_treat_name]] == 1, ],
    by = "fips",
    all.x = TRUE
  )

  # Perform binscatter regression
  binscatter <- binsreg::binsreg(
    y = df_plot[["average"]],
    x = df_plot[[percentile_name]],
    data = df_plot,
    ci = TRUE,
    noplot = TRUE
    )

  # Extract the data for the bins
  binscatter_data <- binscatter$data.plot$`Group Full Sample`$data.bin

  # Create a matrix to store the results
  bins_plot_df <- matrix(0, nrow(binscatter_data), 4)

  # Iterate over each bin
  for (i in seq_len(nrow(binscatter_data))) {
    # Get the left and right endpoints of the bin
    bin_lside <- binscatter_data[["left.endpoint"]][i]
    bin_rside <- binscatter_data[["right.endpoint"]][i]

    # Select the data within the bin
    l_hand_side <- df_plot[[percentile_name]] >= bin_lside
    r_hand_side <- df_plot[[percentile_name]] < bin_rside
    c_interv <- df_plot[(l_hand_side) & (r_hand_side), ]

    # Calculate the mean and confidence interval for the bin
    bins_plot_df[i, 1] <- mean(c_interv[["average"]], na.rm = TRUE)
    within_bin_var <- var(c_interv[["average"]], na.rm = TRUE)
    interv_scope <- qnorm(0.025) * sqrt(within_bin_var / nrow(c_interv))
    bins_plot_df[i, 2] <- bins_plot_df[i, 1] - interv_scope
    bins_plot_df[i, 3] <- bins_plot_df[i, 1] + interv_scope
  }

  # Calculate the x values for the plot
  bins_plot_df[, 4] <- binscatter$data.plot$`Group Full Sample`$data.dots$x

  # Convert the matrix to a tibble
  binscatter_plot_df <- tibble::as_tibble(bins_plot_df)

  # Rename the columns of the tibble
  names(binscatter_plot_df) <- c("fit", "ci.l", "ci.r", "x")

  # Create the binscatter plot
  plot <- ggplot2::ggplot(
    binscatter_plot_df,
    aes(x = x, y = fit)
    ) +
    geom_point(
      color = plot_color
    ) +
    geom_line(
      color = plot_color
    ) +
    geom_errorbar(
      aes(ymin = ci.l, ymax = ci.r),
      color = plot_color
    ) +
    plot_yrange +
    ylab(
      "Time average of the effects"
    ) +
    xlab(
      "Kaitz-p index"
    ) +
    geom_hline(
      yintercept = 0,
      linetype = 5,
      color = "#000000",
      linewidth = 0.5
    ) +
    labs(
      title = plot_title
    )

  # Customize the plot appearance
  plot <- plot +
    theme_classic() +
    theme(
      text = element_text(size = 16, family = "serif"),
      plot.title = element_text(hjust = 0.5)
    )

  if (with_data == TRUE) {
    return(c(plot, binscatter_plot_df))
  } else {
    return(plot)
  }

}

IndividualEffectsCompPlot <- function(df, period, percentiles) {

  # Get the column names of the dataframe
  df_colnames <- df |> colnames()

  # Obtain the policy from treatment name
  policy_name <- df_colnames[grep("first_treatment_", colnames(df))]
  policy <- sub("first_treatment_", "", policy_name)

  # Obtain the outcome from dependend name
  outcome_name <- df_colnames[grep("_tilde", colnames(df))]
  outcome <- sub("_tilde", "", outcome_name)

  # Define the colors for each percentile
  plot_colors <- c(
    "0.10" = "#0098e9",
    "0.25" = "#ff5ca8",
    "0.50" = "#5aa800",
    "0.75" = "#f29318",
    "0.90" = "#B79F00"
  )

  # Select the data for the specified period
  plot_data <- df[df[[paste0("relative_to_treat_", policy)]] == period, ]

  # Create an empty list to store the binscatter plots
  binscatter_collection <- list()

  # Iterate over each percentile
  for (percentile in percentiles) {

    # Get the naming details for the plot
    naming <- TitleAndColors(policy, outcome, percentile)

    # Extract the percentile name
    percentile_name <- naming[[1]]

    # Create the name for the fit column
    fit_name <- paste0(percentile_name, "_fit")

    # Perform binscatter regression
    bin_plot <- binsreg::binsreg(
    y = plot_data[[outcome_name]],
    x = plot_data[[percentile_name]],
    data = plot_data,
    noplot = TRUE
    )

    # Extract the data points for the bins
    bin_plot <- bin_plot$data.plot$`Group Full Sample`$data.dots

    # Select the relevant columns and rename them
    bin_plot <- bin_plot |>
    dplyr::select(x, fit) |>
    setNames(c(percentile_name, fit_name))

    # Add the binscatter plot to the collection
    binscatter_collection[[percentile_name]] <- bin_plot

  }

  # Combine the binscatter plots into a single dataframe
  binscatter_plot_df <- purrr::list_c(binscatter_collection)

  # Extract the plot details
  plot_yrange <- naming[[3]]
  policy_title  <- naming[[4]]
  outcome_title <- naming[[5]]

  # Create the binscatter plot
  plot <- ggplot2::ggplot(binscatter_plot_df) +
    geom_point(
      aes(x = kaitz_pct10,
          y = kaitz_pct10_fit,
          color = "0.10")
      ) +
    geom_line(
      aes(x = kaitz_pct10,
          y = kaitz_pct10_fit,
          color = "0.10")
      ) +
    geom_point(
      aes(x = kaitz_pct25,
          y = kaitz_pct25_fit,
          color = "0.25")
      ) +
    geom_line(
      aes(x = kaitz_pct25,
          y = kaitz_pct25_fit,
          color = "0.25")
      ) +
    geom_point(
      aes(x = kaitz_pct50,
          y = kaitz_pct50_fit,
          color = "0.50")
      ) +
    geom_line(
      aes(x = kaitz_pct50,
          y = kaitz_pct50_fit,
          color = "0.50")
      ) +
    geom_point(
      aes(x = kaitz_pct75,
          y = kaitz_pct75_fit,
          color = "0.75")
      ) +
    geom_line(
      aes(x = kaitz_pct75,
          y = kaitz_pct75_fit,
          color = "0.75")
      ) +
    geom_point(
      aes(x = kaitz_pct90,
          y = kaitz_pct90_fit,
          color = "0.90")
      ) +
    geom_line(
      aes(x = kaitz_pct90,
          y = kaitz_pct90_fit,
          color = "0.90")
      ) +
    plot_yrange +
    ylab(
      "Individual effects"
      ) +
    xlab(
      "Kaitz-p values"
      ) +
    labs(
      title = paste0(
        "Effects of ",
        policy_title,
        " on ",
        outcome_title,
        "\n for different Kaitz-p indices, t = ",
        gsub("V", "", period)
        )
      ) +
    scale_color_manual(
      name = element_blank(), values = plot_colors
      ) +
    geom_hline(
      yintercept = 0, linetype = 5, color = "#000000", linewidth = 0.5
      )

  # Customize the plot appearance
  plot <- plot +
    theme_classic() +
    theme(
      text = element_text(size = 16, family="serif"),
      plot.title = element_text(hjust = 0.5)
    )

  return(plot)

}