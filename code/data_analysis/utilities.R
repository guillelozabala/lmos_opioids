
IndEffects <- function(data,
                       yname,
                       iname,
                       tname,
                       kname,
                       aname,
                       covariates,
                       k_min = 0,
                       k_max = 24,
                       compute_var_me = FALSE,
                       only_full_horizon = TRUE) {

  object <- PrepData(
    data,
    yname,
    iname,
    tname,
    kname,
    aname,
    covariates,
    k_min,
    k_max,
    compute_var_me,
    only_full_horizon
  )

  object <- ComputeProjection(object)

  return(object)

}

PrepData <- function(data,
                     yname,
                     iname,
                     tname,
                     kname,
                     aname,
                     covariates,
                     k_min = 0,
                     k_max = 24,
                     compute_var_me = FALSE,
                     only_full_horizon = TRUE) {

  t_min <- data[[tname]] |> min()
  not_yet_treated <- data[data[[kname]] < k_min, ]

  if (nrow(not_yet_treated) == 0 || fixest:::cpp_isConstant(not_yet_treated[[yname]])) {
    return(dplyr::tibble())
  }

  first_stage <- fixest::feols(
    stats::as.formula(
      paste0(yname, " ~ ", covariates, " |", iname, " + ", tname)
    ),
    data = not_yet_treated,
    combine.quick = FALSE,
    warn = FALSE,
    notes = FALSE)

  data[[paste0(yname, "_hat")]] <- stats::predict(first_stage, newdata = data)

  data[[paste0(yname, "_tilde")]] <- data[[yname]] -
    data[[paste0(yname, "_hat")]]

  df_indcp <- data[!is.na(data[paste0(yname, "_tilde")]), ]

  t_min <- df_indcp[[tname]] |> min()
  t_max <- df_indcp[[tname]] |> max()

  a_min <- df_indcp[[aname]] |> min()
  a_max <- df_indcp[[aname]] |> max()

  info <- list(yname = yname,
               iname = iname,
               tname = tname,
               kname = kname,
               aname = aname,
               t_min = t_min,
               t_max = t_max,
               k_min = k_min,
               k_max = k_max,
               a_min = a_min,
               a_max = a_max,
               ytildename = paste0(yname, "_tilde"),
               compute_var_me = compute_var_me,
               only_full_horizon = only_full_horizon)
  
  object <- list(df_indcp = df_indcp, info = info)
  
  class(object) <- "indcp"
  
  return(object)
  
}

ComputeProjection <- function(object) {
  
  kname <- object$info$kname
  aname <- object$info$aname
  ytildename <- object$info$ytildename
  k_min <- object$info$k_min
  k_max <- object$info$k_max
  
  # dplyr::between() - This is a shortcut for x >= left & x <= right
  # dplyr::arrange() - Orders the rows of a data frame by the values of 
  
  # Aggregated Data by treatment timing
  object$aggregated <- object$df_indcp |>
    dplyr::filter(dplyr::between(!!rlang::sym(kname), k_min, k_max)) |>
    dplyr::summarize(!!paste0("mean_", ytildename) := mean(!!rlang::sym(ytildename)),
                     !!paste0("sd_", ytildename) := stats::sd(!!rlang::sym(ytildename)),
                     n = dplyr::n(),
                     .by = c(aname, kname)) |>
    dplyr::arrange(!!rlang::sym(aname),
                   !!rlang::sym(kname))
  
  # Choose only the cohorts with full horizon
  if (object$info$only_full_horizon) {
    object$aggregated <- object$aggregated |>
      dplyr::summarize(n_k = dplyr::n(),
                       .by = c(aname)) |>
      dplyr::filter(!!rlang::sym("n_k") == k_max - k_min + 1) |>
      dplyr::select(-dplyr::any_of("n_k")) |>
      dplyr::left_join(object$aggregated, by = c(aname))
    
    object$info$a_min <- object$aggregated[[aname]] |> min()
    object$info$a_max <- object$aggregated[[aname]] |> max()
  }
  
  # Compute Variance of epsilon
  if (object$info$compute_var_me) {
    var_epsilon <- VarEpsilonB(object, k_max = k_max)
    
    object$aggregated <- object$aggregated |>
      dplyr::left_join(var_epsilon, by = c(aname, kname)) |>
      dplyr::mutate(!!paste0("var_", ytildename, "_estimated")
                    := (!!rlang::sym(paste0("sd_", ytildename)))^2 - (!!rlang::sym("sd_epsilon"))^2,
                    !!paste0("sd_", ytildename, "_estimated")
                    := sqrt(dplyr::if_else(!!rlang::sym(paste0("var_", ytildename, "_estimated")) > 0,
                                           !!rlang::sym(paste0("var_", ytildename, "_estimated")), 0))) |>
      dplyr::select(-dplyr::any_of(paste0("var_", ytildename, "_estimated")))
  }
  
  return(object)
}

VarEpsilonB <- function(object, b, k_max) {
  
  k_min <- object$info$k_min
  a_min <- object$info$a_min
  a_max <- object$info$a_max
  t_min <- object$info$t_min
  t_max <- object$info$t_max
  
  a_start <- max(a_min, t_min - k_min + 1)
  a_end <- min(a_max, t_max - k_min - 1 - k_max)
  
  if (a_start > a_end) {
    return(dplyr::tibble())
  }
  
  result <- purrr::map2(rep(a_start:a_end, each = k_max - k_min + 1),
                        rep(k_min:k_max, times = a_end - a_start + 1),
                        ~VarEpsilonAk(object, .x, .y)) |>
    purrr::list_rbind()
  
  return(result)
}

VarEpsilonAk <- function(object, a, k) {
  
  iname <- object$info$iname
  tname <- object$info$tname
  aname <- object$info$aname
  kname <- object$info$kname
  ytildename <- object$info$ytildename
  
  df_var <- object$df_indcp
  
  epsilon_right <- df_var |>
    dplyr::filter(!!rlang::sym(aname) > a + k,
                  !!rlang::sym(tname) < a + k) |>
    dplyr::summarize(epsilon_right = mean(!!rlang::sym(ytildename)),
                     .by = !!rlang::sym(iname))
  
  sum_epsilon <- df_var |>
    dplyr::filter(!!rlang::sym(aname) > a + k,
                  !!rlang::sym(tname) == a + k) |>
    dplyr::left_join(epsilon_right, by = c(iname)) |>
    dplyr::mutate(epsilon_hat = !!rlang::sym(ytildename) - epsilon_right) |>
    dplyr::filter(!is.na(!!rlang::sym("epsilon_hat"))) |>
    dplyr::summarize(sd_epsilon = stats::sd(!!rlang::sym("epsilon_hat")),
                     n = dplyr::n())
  
  result <- dplyr::tibble(!!aname := a,
                          !!kname := k,
                          "sd_epsilon" = sum_epsilon$sd_epsilon)
  return(result)
  
}
